defmodule Prism.DeliveryCallbackContractTest.CaptureTransport do
  @behaviour Prism.EventBus.Transport.Behaviour

  def publish(stream, payload, maxlen, headers) do
    send(Application.fetch_env!(:prism, :capture_transport_test_pid), {
      :published,
      stream,
      payload,
      maxlen,
      headers
    })

    case Application.get_env(:prism, :capture_transport_result, :ok) do
      result when is_function(result, 0) -> result.()
      result -> result
    end
  end

  def create_consumer_group(_stream, _consumer_group), do: :ok
  def read_batch(_stream, _consumer_group, _consumer_name, _block_ms, _batch_size), do: {:ok, []}
  def ack(_stream, _consumer_group, _ids), do: :ok
  def claim_stale(_stream, _consumer_group, _consumer_name, _idle_ms, _count), do: []
  def system_name, do: "capture"
end

defmodule Prism.DeliveryCallbackContractTest do
  use ExUnit.Case, async: false

  alias Interchat.TrustAndSafety.V2.PrismDeliveryCallback

  @action_id "019f603b-227b-78b2-909e-47717b9b5765"

  setup do
    previous_backend = Application.get_env(:prism, :event_bus_transport_backend)
    previous_pid = Application.get_env(:prism, :capture_transport_test_pid)
    previous_result = Application.get_env(:prism, :capture_transport_result)
    previous_handoff_base = Application.get_env(:prism, :prism_handoff_retry_base_ms)
    previous_include_parent = Application.get_env(:prism, :callback_include_parent_message_id)
    previous_preflight = Application.get_env(:prism, :preflight_batching_enabled)

    Application.put_env(
      :prism,
      :event_bus_transport_backend,
      Prism.DeliveryCallbackContractTest.CaptureTransport
    )

    Application.put_env(:prism, :capture_transport_test_pid, self())
    Application.put_env(:prism, :prism_handoff_retry_base_ms, 0)

    on_exit(fn ->
      restore_env(:event_bus_transport_backend, previous_backend)
      restore_env(:capture_transport_test_pid, previous_pid)
      restore_env(:capture_transport_result, previous_result)
      restore_env(:prism_handoff_retry_base_ms, previous_handoff_base)
      restore_env(:callback_include_parent_message_id, previous_include_parent)
      restore_env(:preflight_batching_enabled, previous_preflight)
    end)

    :ok
  end

  test "publishes the canonical callback with action/message identity and occurrence time" do
    before_seconds = System.system_time(:second)

    assert :ok =
             Prism.Helpers.publish_delivery_callback(
               @action_id,
               "message-1",
               :MESSAGE_STATE_ACTIVE
             )

    assert_receive {:published, "events.prism.delivery.v2", payload, _maxlen, headers}
    assert headers["partition-key"] == @action_id
    assert headers["ce_type"] == "interchat.prism.delivery.v2"
    assert headers["content-type"] == "application/protobuf"

    assert {:ok, callback} = PrismDeliveryCallback.decode(payload)
    assert callback.action_id == @action_id
    assert callback.message_id == "message-1"
    assert callback.state == :MESSAGE_STATE_ACTIVE
    assert callback.failure_code == ""
    assert callback.occurred_at.seconds >= before_seconds
    assert callback.occurred_at.nanos in 0..999_999_999
  end

  test "retry summaries retain action, batch, and parent-message identity" do
    target = %{
      "polarizer_action_id" => @action_id,
      "webhook_id" => "webhook-1",
      "channel_id" => "channel-1",
      "guild_id" => "guild-1"
    }

    assert :ok =
             Prism.DiscordWorker.Callbacks.publish_partial(
               "execute",
               target,
               "batch-1",
               "message-1",
               "delivered-message-1",
               nil
             )

    assert_receive {:published, "events.bus", summary_payload, _maxlen, _headers}
    summary = Jason.decode!(summary_payload)
    assert summary["action_id"] == @action_id
    assert summary["batch_id"] == "batch-1"
    assert summary["parent_message_id"] == "message-1"

    assert_receive {:published, "events.prism.delivery.v2", callback_payload, _maxlen, _headers}
    assert {:ok, callback} = PrismDeliveryCallback.decode(callback_payload)
    assert callback.action_id == @action_id
    assert callback.message_id == "message-1"
  end

  test "batch callback accepts a Discord snowflake as parent-message identity" do
    Application.put_env(:prism, :callback_include_parent_message_id, true)
    Application.put_env(:prism, :preflight_batching_enabled, false)
    parent_message_id = "1532370911045353512"
    now = System.system_time(:millisecond)

    assert :ok =
             Prism.FanoutBroadway.Batch.process_batch(
               "execute",
               "batch-snowflake",
               %{},
               [],
               now,
               now,
               parent_message_id,
               %{},
               nil,
               0
             )

    assert_receive {:published, "events.bus", summary_payload, _maxlen, _headers}
    summary = Jason.decode!(summary_payload)
    assert summary["parent_message_id"] == parent_message_id
  end

  test "authoritative callback failure is returned after bounded broker retries" do
    Application.put_env(:prism, :capture_transport_result, {:error, :broker_unavailable})

    assert {:error, :broker_unavailable} =
             Prism.Helpers.publish_delivery_callback(
               @action_id,
               "message-1",
               :MESSAGE_STATE_ACTIVE
             )

    for _ <- 1..3 do
      assert_receive {:published, "events.prism.delivery.v2", _payload, _maxlen, _headers}
    end
  end

  test "raw retry publication preserves the original Kafka contract and key" do
    headers = %{
      "ce_specversion" => "1.0",
      "ce_source" => "/polarizer",
      "ce_type" => "fun.interchat.prism.job",
      "content-type" => "application/protobuf"
    }

    assert :ok =
             Prism.EventBus.Publisher.publish_raw(
               "prism.stream.jobs",
               <<1, 2, 3>>,
               headers: headers,
               key: "HUB:hub-1"
             )

    assert_receive {:published, "prism.stream.jobs", <<1, 2, 3>>, _maxlen, published_headers}
    assert published_headers["partition-key"] == "HUB:hub-1"
    assert published_headers["ce_source"] == "/polarizer"
    assert published_headers["ce_type"] == "fun.interchat.prism.job"
  end

  test "invalid Kafka jobs cross the DLQ broker boundary before acknowledgement" do
    message = %Broadway.Message{
      data: <<0, 1, 2>>,
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{
        key: "HUB:hub-1",
        headers: [
          {"ce_source", "/polarizer"},
          {"ce_type", "fun.interchat.prism.job"}
        ]
      },
      status: {:failed, {:invalid_contract, :invalid_protobuf}}
    }

    assert [^message] = Prism.FanoutBroadway.handle_failed([message], %{})

    assert_receive {:published, "prism.stream.jobs.dlq", <<0, 1, 2>>, _maxlen, headers}
    assert headers["partition-key"] == "HUB:hub-1"
    assert headers["prism-error-code"] == "invalid_protobuf"
  end

  test "failed jobs cross a broker-acknowledged Kafka retry boundary" do
    message = failed_job_message()
    before_ms = System.system_time(:millisecond)

    assert [^message] = Prism.FanoutBroadway.handle_failed([message], %{})

    assert_receive {:published, "prism.stream.jobs.retry", <<1, 2, 3>>, _maxlen, headers}
    assert headers["partition-key"] == "HUB:hub-1"
    assert headers["ce_source"] == "/polarizer"
    assert headers["prism-original-topic"] == "prism.stream.jobs"
    assert headers["prism-retry-attempt"] == "1"
    assert headers["prism-retry-reason"] == "processing_failed"
    assert String.to_integer(headers["prism-not-before-ms"]) >= before_ms + 1_000
  end

  test "a failed retry publish is retried before handle_failed permits acknowledgement" do
    {:ok, results} = Agent.start_link(fn -> [{:error, :broker_unavailable}, :ok] end)

    Application.put_env(:prism, :capture_transport_result, fn ->
      Agent.get_and_update(results, fn [result | rest] -> {result, rest} end)
    end)

    message = failed_job_message()
    assert [^message] = Prism.FanoutBroadway.handle_failed([message], %{})

    for _ <- 1..2 do
      assert_receive {:published, "prism.stream.jobs.retry", <<1, 2, 3>>, _maxlen, _headers}
    end
  end

  test "retry deadlines are calculated from durable Kafka metadata" do
    metadata = %{headers: [{"prism-not-before-ms", "1500"}]}
    assert Prism.FanoutBroadway.retry_delay_ms(metadata, 1_000) == 500
    assert Prism.FanoutBroadway.retry_delay_ms(metadata, 2_000) == 0
  end

  defp failed_job_message do
    %Broadway.Message{
      data: <<1, 2, 3>>,
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{
        topic: "prism.stream.jobs",
        key: "HUB:hub-1",
        headers: [
          {"ce_source", "/polarizer"},
          {"ce_type", "fun.interchat.prism.job"}
        ]
      },
      status: {:failed, {:processing_failed, "callback unavailable"}}
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:prism, key)
  defp restore_env(key, value), do: Application.put_env(:prism, key, value)
end
