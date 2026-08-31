defmodule Prism.KafkaConfigTest do
  use ExUnit.Case, async: true

  defp getter(values), do: fn name -> Map.get(values, name) end

  test "requires explicit TLS and SASL settings" do
    assert {:error, "KAFKA_BROKERS is required"} = Prism.KafkaConfig.from_env(getter(%{}))
  end

  test "rejects malformed brokers and unsupported mechanisms" do
    values = %{
      "KAFKA_BROKERS" => "kafka:9093",
      "KAFKA_TLS_CA" => "/etc/kafka/ca.crt",
      "KAFKA_SASL_USERNAME" => "prism",
      "KAFKA_SASL_PASSWORD" => "secret",
      "KAFKA_SASL_MECHANISM" => "PLAIN"
    }

    assert {:error, message} = Prism.KafkaConfig.from_env(getter(values))
    assert message =~ "KAFKA_SASL_MECHANISM"

    malformed = Map.put(values, "KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")

    assert {:error, _} =
             Prism.KafkaConfig.from_env(getter(Map.put(malformed, "KAFKA_BROKERS", "kafka")))
  end

  test "builds SCRAM TLS client options and disables topic creation" do
    values = %{
      "KAFKA_BROKERS" => "kafka-a:9093, kafka-b:9094",
      "KAFKA_TLS_CA" => "/etc/kafka/ca.crt",
      "KAFKA_SASL_USERNAME" => "prism",
      "KAFKA_SASL_PASSWORD" => "secret"
    }

    assert {:ok, config} = Prism.KafkaConfig.from_env(getter(values))
    assert config.brokers == [{"kafka-a", 9093}, {"kafka-b", 9094}]
    assert config.brod_client_config[:allow_topic_auto_creation] == false
    assert config.client_config[:sasl] == {:scram_sha_512, "prism", "secret"}
  end
end
