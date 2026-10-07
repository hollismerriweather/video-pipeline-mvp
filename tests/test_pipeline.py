import pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]))
from video_pipeline.pipeline import headers
class T(unittest.TestCase):
 def test_user_agent(self): self.assertTrue(headers("x")["User-Agent"])
if __name__=="__main__":unittest.main()
