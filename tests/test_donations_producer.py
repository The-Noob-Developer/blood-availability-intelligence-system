import unittest
from unittest.mock import patch

from streaming.donations import donations_producer as producer_module


class DonationsProducerTests(unittest.TestCase):
    def test_get_producer_reuses_single_instance(self):
        producer_module.producer = None

        with patch.object(producer_module, "KafkaProducer", return_value=object()) as kafka_cls:
            first = producer_module.get_producer()
            second = producer_module.get_producer()

        self.assertIs(first, second)
        self.assertEqual(kafka_cls.call_count, 1)


if __name__ == "__main__":
    unittest.main()
