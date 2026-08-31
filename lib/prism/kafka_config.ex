defmodule Prism.KafkaConfig do
  @moduledoc """
  Strict Kafka TLS/SASL configuration for non-test environments.
  """

  @mechanism "SCRAM-SHA-512"

  def from_env(getter \\ &System.get_env/1) do
    with {:ok, brokers} <- required(getter, "KAFKA_BROKERS"),
         {:ok, ca_path} <- required(getter, "KAFKA_TLS_CA"),
         {:ok, username} <- required(getter, "KAFKA_SASL_USERNAME"),
         {:ok, password} <- required(getter, "KAFKA_SASL_PASSWORD"),
         {:ok, mechanism} <- validate_mechanism(getter.("KAFKA_SASL_MECHANISM")),
         {:ok, endpoints} <- parse_brokers(brokers) do
      {:ok,
       %{
         brokers: endpoints,
         client_config: [
           ssl: [verify: :verify_peer, cacertfile: String.to_charlist(ca_path)],
           sasl: {:scram_sha_512, username, password},
           extra_sock_opts: [keepalive: true]
         ],
         brod_client_config: [
           ssl: [verify: :verify_peer, cacertfile: String.to_charlist(ca_path)],
           sasl: {:scram_sha_512, username, password},
           allow_topic_auto_creation: false,
           extra_sock_opts: [keepalive: true]
         ],
         mechanism: mechanism
       }}
    end
  end

  def from_env!(getter \\ &System.get_env/1) do
    case from_env(getter) do
      {:ok, config} -> config
      {:error, reason} -> raise "invalid Kafka configuration: #{reason}"
    end
  end

  defp required(getter, name) do
    case getter.(name) do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> {:error, "#{name} is required"}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, "#{name} is required"}
    end
  end

  defp validate_mechanism(nil), do: {:ok, @mechanism}
  defp validate_mechanism(""), do: {:ok, @mechanism}
  defp validate_mechanism(@mechanism), do: {:ok, @mechanism}

  defp validate_mechanism(value),
    do: {:error, "KAFKA_SASL_MECHANISM must be #{@mechanism}, got #{inspect(value)}"}

  defp parse_brokers(value) do
    endpoints = value |> String.split(",", trim: true) |> Enum.map(&String.trim/1)

    if endpoints == [] or Enum.any?(endpoints, &invalid_broker?/1) do
      {:error, "KAFKA_BROKERS must contain comma-separated host:port endpoints"}
    else
      {:ok, Enum.map(endpoints, &parse_broker!/1)}
    end
  end

  defp invalid_broker?(broker) do
    case String.split(broker, ":", parts: 2) do
      [host, port] -> host == "" or not valid_port?(port)
      _ -> true
    end
  end

  defp valid_port?(port) do
    case Integer.parse(port) do
      {value, ""} -> value in 1..65_535
      _ -> false
    end
  end

  defp parse_broker!(broker) do
    [host, port] = String.split(broker, ":", parts: 2)
    {host, String.to_integer(port)}
  end
end
