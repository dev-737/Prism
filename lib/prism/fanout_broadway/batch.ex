defmodule Prism.FanoutBroadway.Batch do
  @moduledoc """
  Batch processing: fans out webhook targets with bounded concurrency,
  aggregates results, stores reply index, and publishes callbacks to Redis.
  """
  alias Prism.Helpers

  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Processes a batch of webhook targets asynchronously, aggregates results,
  publishes callback stream and SSE events, and broadcasts to pg.
  """
  @spec process_batch(
          String.t(),
          String.t(),
          map(),
          [map()],
          integer(),
          integer(),
          String.t() | nil,
          map(),
          String.t() | nil,
          integer()
        ) :: :ok
  def process_batch(
        action,
        batch_id,
        discord_payload,
        targets,
        polled_at,
        enqueued_at,
        parent_message_id,
        _payload_metadata,
        root_hub_id,
        shard_index
      ) do
    include_parent_message_id = Prism.Config.callback_include_parent_message_id?()

    queue_time = polled_at - enqueued_at

    Logger.debug(
      "Started batch #{batch_id} (Queue Time: #{queue_time}ms, Targets: #{length(targets)})"
    )

    queue_warn = Prism.Config.queue_time_warn_ms()

    if queue_time > queue_warn do
      Logger.warning(
        "High queue time for batch #{batch_id}: #{queue_time}ms (Targets: #{length(targets)})"
      )
    end

    preflight_enabled = Prism.Config.preflight_batching_enabled?()
    defer_threshold = Prism.Config.rate_limit_defer_threshold_ms()
    batch_max_concurrency = Prism.Config.batch_max_concurrency()

    {targets_to_process, preflight_done_targets, _preflight_deferred_targets} =
      if preflight_enabled do
        case Prism.FanoutBroadway.Preflight.run(targets, action, batch_id) do
          {:ok, preflights} ->
            {ready, deferred, done} =
              Enum.reduce(preflights, {[], [], []}, fn pf, {r_acc, d_acc, dn_acc} ->
                case pf.preflight.checkpoint do
                  {:done} ->
                    {r_acc, d_acc, [{:done, pf} | dn_acc]}

                  {:ok, msg_id} ->
                    {r_acc, d_acc, [{:ok, msg_id, pf} | dn_acc]}

                  :not_found ->
                    case pf.preflight.rate_limit do
                      {:blocked, ttl} when ttl > defer_threshold ->
                        {r_acc, [%{pf | delay_ms: ttl} | d_acc], dn_acc}

                      _ ->
                        {[pf | r_acc], d_acc, dn_acc}
                    end
                end
              end)

            parent_msg_id = if action == "execute", do: parent_message_id, else: nil

            for pf <- deferred do
              target = pf.target
              webhook_id = Map.get(target, "webhook_id")
              message_id = Map.get(target, "message_id")

              base_url =
                "#{Prism.Config.discord_base_url()}/api/webhooks/#{webhook_id}/#{Map.get(target, "webhook_token")}"

              thread_id = Map.get(target, "thread_id")

              case Prism.DiscordWorker.HTTP.build_request(action, base_url, message_id, thread_id) do
                {:ok, method, url} ->
                  body =
                    case Helpers.merge_overrides(discord_payload, target, action) do
                      nil -> nil
                      merged -> Jason.encode_to_iodata!(merged)
                    end

                  Prism.DiscordWorker.Retry.spawn_retry(
                    action,
                    target,
                    method,
                    url,
                    [{"Content-Type", "application/json"}],
                    body,
                    webhook_id,
                    message_id,
                    batch_id,
                    pf.delay_ms,
                    1,
                    parent_msg_id,
                    :rate_limited
                  )

                {:error, _} ->
                  :ok
              end
            end

            {ready, done, []}

          {:error, _reason} ->
            {targets, [], []}
        end
      else
        {targets, [], []}
      end

    pre_encoded_body =
      if action != "delete" and is_map(discord_payload) do
        Jason.encode_to_iodata!(discord_payload)
      else
        nil
      end

    OpenTelemetry.Tracer.with_span "prism.worker.process_batch" do
      OpenTelemetry.Tracer.set_attributes([
        {:batch_id, batch_id},
        {:action, action},
        {:target_count, length(targets)},
        {:shard_index, shard_index}
      ])

      ctx = OpenTelemetry.Ctx.get_current()

      results =
        case targets_to_process do
          [] ->
            []

          [single_pf] when is_map(single_pf) and preflight_enabled ->
            OpenTelemetry.Ctx.attach(ctx)

            result =
              Prism.DiscordWorker.process_target(
                action,
                single_pf.target,
                discord_payload,
                batch_id,
                polled_at,
                enqueued_at,
                parent_message_id,
                preflight: single_pf.preflight,
                skip_checkpoint_write: true,
                pre_encoded_body: pre_encoded_body
              )

            [{:ok, result}]

          [single_target] ->
            OpenTelemetry.Ctx.attach(ctx)

            result =
              Prism.DiscordWorker.process_target(
                action,
                single_target,
                discord_payload,
                batch_id,
                polled_at,
                enqueued_at,
                parent_message_id,
                pre_encoded_body: pre_encoded_body
              )

            [{:ok, result}]

          multiple_targets ->
            Task.async_stream(
              multiple_targets,
              fn item ->
                OpenTelemetry.Ctx.attach(ctx)

                if preflight_enabled do
                  Prism.DiscordWorker.process_target(
                    action,
                    item.target,
                    discord_payload,
                    batch_id,
                    polled_at,
                    enqueued_at,
                    parent_message_id,
                    preflight: item.preflight,
                    skip_checkpoint_write: true,
                    pre_encoded_body: pre_encoded_body
                  )
                else
                  Prism.DiscordWorker.process_target(
                    action,
                    item,
                    discord_payload,
                    batch_id,
                    polled_at,
                    enqueued_at,
                    parent_message_id,
                    pre_encoded_body: pre_encoded_body
                  )
                end
              end,
              max_concurrency: batch_max_concurrency,
              timeout: Prism.Config.task_timeout_ms()
            )
            |> Enum.to_list()
        end

      # Post-flight: batch checkpoint writes for successful targets
      if preflight_enabled do
        checkpoint_ttl = to_string(Prism.Config.checkpoint_ttl_seconds())

        checkpoint_commands =
          targets_to_process
          |> Enum.zip(results)
          |> Enum.filter(fn {_target_pf, result_tuple} ->
            case result_tuple do
              {:ok, {:ok, _}} -> true
              {:ok, result} when is_tuple(result) -> elem(result, 0) == :ok
              _ -> false
            end
          end)
          |> Enum.map(fn {item, {:ok, worker_result}} ->
            ck_result =
              case worker_result do
                {:ok, msg_id} when is_binary(msg_id) -> msg_id
                {:ok, _} -> "done"
                _ -> "done"
              end

            webhook_id = item.webhook_id

            ck =
              Helpers.checkpoint_key(
                action,
                batch_id,
                webhook_id,
                Map.get(item.target, "polarizer_action_id")
              )

            ["SETEX", ck, checkpoint_ttl, ck_result]
          end)

        if checkpoint_commands != [] do
          Helpers.redix_pipeline(checkpoint_commands)
        end
      end

      # Build combined target list and results for aggregation
      {aggregation_targets, aggregation_results} =
        if preflight_enabled do
          ready_targets = Enum.map(targets_to_process, fn pf -> pf.target end)

          {done_target_pfs, done_result_pfs} =
            preflight_done_targets
            |> Enum.map(fn
              {:done, pf} -> {pf.target, {:ok, {:ok, nil}}}
              {:ok, msg_id, pf} -> {pf.target, {:ok, {:ok, msg_id}}}
            end)
            |> Enum.unzip()

          {ready_targets ++ done_target_pfs, results ++ done_result_pfs}
        else
          {targets_to_process, results}
        end

      {successes, failures} =
        aggregation_targets
        |> Enum.zip(aggregation_results)
        |> Enum.reduce({[], []}, fn {target, result_tuple}, {succ_acc, fail_acc} ->
          worker_result =
            case result_tuple do
              {:ok, res} -> res
              _ -> {:error, :task_crashed}
            end

          webhook_id = Map.get(target, "webhook_id") || "unknown"
          conn_id = Map.get(target, "connection_id")
          hub_id = Map.get(target, "hub_id")
          channel_id = Map.get(target, "channel_id")
          guild_id = Map.get(target, "guild_id")

          base_info = %{
            "webhook_id" => webhook_id,
            "message_id" => Map.get(target, "message_id"),
            "connection_id" => conn_id,
            "hub_id" => hub_id,
            "channel_id" => channel_id,
            "guild_id" => guild_id
          }

          base_info = :maps.filter(fn _, v -> v != nil end, base_info)

          case worker_result do
            {:ok, msg_id} ->
              succ_info =
                if msg_id, do: Map.put(base_info, "message_id", msg_id), else: base_info

              {[succ_info | succ_acc], fail_acc}

            {:error, reason} ->
              {error_string, error_type, extra} = Prism.ErrorMapping.to_error_info(reason)

              fail_info =
                base_info
                |> Map.put("error", error_string)
                |> Map.put("error_type", error_type)
                |> Map.merge(extra)

              {succ_acc, [fail_info | fail_acc]}
          end
        end)

      ok_count = length(successes)
      fail_count = length(failures)
      batch_time = :os.system_time(:millisecond) - polled_at
      parent_log = if parent_message_id, do: " (Parent Msg: #{parent_message_id})", else: ""

      Logger.debug(
        "Batch #{batch_id}#{parent_log} done in #{batch_time}ms: #{ok_count} ok, #{fail_count} unsuccessful"
      )

      if action == "execute" and not is_nil(parent_message_id) and
           Prism.Config.reply_index_enabled?() do
        store_reply_index(parent_message_id, successes)
      end

      polarizer_action_id =
        targets
        |> List.first(%{})
        |> Map.get("polarizer_action_id")

      payload_map = %{
        batch_id: batch_id,
        action_id: polarizer_action_id,
        status: "success",
        action: action,
        message_ids: Enum.reverse(successes),
        failures: Enum.reverse(failures)
      }

      payload_map =
        if is_binary(parent_message_id) and parent_message_id != "" and
             (include_parent_message_id or not is_nil(polarizer_action_id)) do
          Map.put(payload_map, :parent_message_id, parent_message_id)
        else
          payload_map
        end

      delivery_state =
        if ok_count > 0,
          do: {:MESSAGE_STATE_ACTIVE, ""},
          else: {:MESSAGE_STATE_DELIVERY_FAILED, "NO_TARGET_DELIVERED"}

      publish_completion_callbacks(
        payload_map,
        polarizer_action_id,
        parent_message_id,
        delivery_state
      )

      events_stream = Prism.EventBus.Config.events_stream()
      Logger.debug("Published callback to #{events_stream} for batch #{batch_id}#{parent_log}")

      # After publishing callback, notify Beacon via event bus
      if action == "execute" and root_hub_id do
        Prism.EventBus.publish(events_stream,
          type: Prism.EventBus.Config.broadcast_event_type(),
          data: %{
            batch_id: batch_id,
            action: action,
            ok_count: ok_count,
            fail_count: fail_count,
            parent_message_id: parent_message_id,
            hub_id: root_hub_id,
            timestamp: :os.system_time(:millisecond)
          }
        )
      end

      Prism.AsyncBatchCounter.add_processed(length(targets))
    end
  end

  @doc false
  def publish_completion_callbacks(payload_map, action_id, message_id, {state, failure_code}) do
    case Helpers.publish_callback(payload_map) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to publish batch callback for #{payload_map.batch_id}: #{inspect(reason)}. " <>
            "Discord deliveries will not be replayed."
        )
    end

    case Helpers.publish_delivery_callback(action_id, message_id, state, failure_code) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Authoritative delivery callback failed for batch #{payload_map.batch_id}: " <>
            "#{inspect(reason)}. Discord deliveries will not be replayed."
        )
    end

    :ok
  end

  @doc """
  Spawns `process_batch/10` as an async task under `Prism.TaskSup`.
  Returns immediately so the Broadway processor can pull the next message.

  On task crash, re-enqueues the full batch payload to the delayed queue.
  """
  @spec spawn_async_batch(
          String.t(),
          String.t(),
          map(),
          [map()],
          integer(),
          integer(),
          String.t() | nil,
          map(),
          String.t() | nil,
          integer()
        ) :: :ok
  def spawn_async_batch(
        action,
        batch_id,
        discord_payload,
        targets,
        polled_at,
        enqueued_at,
        parent_message_id,
        metadata,
        hub_id,
        shard_index
      ) do
    case Task.Supervisor.start_child(Prism.TaskSup, fn ->
           Prism.AsyncBatchCounter.increment()

           try do
             process_batch(
               action,
               batch_id,
               discord_payload,
               targets,
               polled_at,
               enqueued_at,
               parent_message_id,
               metadata,
               hub_id,
               shard_index
             )
           rescue
             e ->
               Logger.error(
                 "Async batch #{batch_id} crashed: #{Exception.message(e)}\n" <>
                   Exception.format_stacktrace(__STACKTRACE__)
               )

               payload =
                 build_re_enqueue_payload(
                   action,
                   batch_id,
                   discord_payload,
                   targets,
                   parent_message_id,
                   metadata,
                   hub_id,
                   shard_index
                 )

               Prism.DelayedQueue.enqueue(payload, 5_000)
           after
             Prism.AsyncBatchCounter.decrement()
           end
         end) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to start async task for batch #{batch_id}: #{inspect(reason)}. " <>
            "Re-enqueueing to delayed queue (200ms)."
        )

        payload =
          build_re_enqueue_payload(
            action,
            batch_id,
            discord_payload,
            targets,
            parent_message_id,
            metadata,
            hub_id,
            shard_index
          )

        Prism.DelayedQueue.enqueue(payload, 200)
    end
  end

  defp build_re_enqueue_payload(
         action,
         batch_id,
         discord_payload,
         targets,
         parent_message_id,
         metadata,
         hub_id,
         shard_index
       ) do
    %{
      "action" => action,
      "batch_id" => batch_id,
      "payload" => discord_payload,
      "targets" => targets,
      "message_id" => parent_message_id,
      "metadata" => metadata,
      "hub_id" => hub_id,
      "shard_index" => shard_index
    }
  end

  defp store_reply_index(parent_message_id, successes) when is_binary(parent_message_id) do
    prism_prefix = Prism.Config.prism_prefix()
    reply_index_ttl = Integer.to_string(Prism.Config.reply_index_ttl_seconds())
    reply_key = "#{prism_prefix}:targets:#{parent_message_id}"

    commands =
      Enum.flat_map(successes, fn success ->
        channel_id = Map.get(success, "channel_id")
        broadcast_id = Map.get(success, "message_id")

        if is_binary(channel_id) and is_binary(broadcast_id) do
          [
            ["HSET", reply_key, channel_id, broadcast_id],
            [
              "SETEX",
              "#{prism_prefix}:origin:#{broadcast_id}",
              reply_index_ttl,
              parent_message_id
            ]
          ]
        else
          []
        end
      end)

    if commands != [] do
      commands = commands ++ [["EXPIRE", reply_key, reply_index_ttl]]
      Helpers.redix_pipeline(commands)
    end
  end

  defp store_reply_index(_parent_message_id, _successes), do: :ok
end
