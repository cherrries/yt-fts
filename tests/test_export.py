import unittest
import os
from yt_fts.export import export_fts

class TestExportFts(unittest.TestCase):
    def test_export_fts_csv(self):
        # Setup
        text = "test"
        scope = "all"
        format = "csv"
        channel_id = "1"
        video_id = None 

        # Call function
        result = export_fts(text, scope, format, channel_id, video_id)

        # Check that a CSV file was created
        self.assertTrue(os.path.exists(result))
        self.assertTrue(result.endswith('.csv'))

        # Cleanup
        os.remove(result)

    def test_export_fts_html(self):
        # Setup
        text = "test"
        scope = "all"
        format = "html"
        channel_id = "1"
        video_id = None 

        # Call function
        result = export_fts(text, scope, format, channel_id, video_id)

        # Check that an HTML file was created
        self.assertTrue(os.path.exists(result))
        self.assertTrue(result.endswith('.html'))

        # Cleanup
        os.remove(result)

    def test_export_fts_md(self):
        # Setup
        text = "test"
        scope = "all"
        format = "md"
        channel_id = "1"
        video_id = None 

        # Call function
        result = export_fts(text, scope, format, channel_id, video_id)

        # Check that a Markdown file was created
        self.assertTrue(os.path.exists(result))
        self.assertTrue(result.endswith('.md'))

        # Cleanup
        os.remove(result)

if __name__ == '__main__':
    unittest.main()