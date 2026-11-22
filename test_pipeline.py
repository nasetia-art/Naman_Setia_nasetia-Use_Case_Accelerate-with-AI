import tempfile
import unittest
from pathlib import Path

from run_pipeline import (
    aggregation_agent,
    cleansing_agent,
    ingestion_agent,
    reporting_agent,
)


class PipelineTests(unittest.TestCase):
    def test_sample_data_runs_through_all_layers(self):
        repository = Path(__file__).resolve().parents[1]
        raw_dir = repository / "data" / "raw"

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            bronze_rows = ingestion_agent(raw_dir, output_dir / "bronze")
            silver_rows, rejected_rows = cleansing_agent(
                bronze_rows, output_dir / "silver"
            )
            gold_rows = aggregation_agent(silver_rows, output_dir / "gold")
            reporting_agent(
                silver_rows,
                rejected_rows,
                gold_rows,
                output_dir / "reports",
            )

            self.assertEqual(len(silver_rows), 4)
            self.assertEqual(len(rejected_rows), 2)
            self.assertEqual(len(gold_rows), 4)

            self.assertTrue(
                (output_dir / "bronze" / "retail_sales.csv").exists()
            )
            self.assertTrue(
                (output_dir / "silver" / "sales_silver.csv").exists()
            )
            self.assertTrue(
                (output_dir / "gold" / "daily_store_sales.csv").exists()
            )
            self.assertTrue(
                (output_dir / "reports" / "sales_report.md").exists()
            )


if __name__ == "__main__":
    unittest.main()