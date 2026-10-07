import argparse,json,os,time
from concurrent.futures import ThreadPoolExecutor,as_completed
from urllib.request import Request,urlopen
def headers(key): return {'Authorization':'Bearer '+key,'Content-Type':'application/json','User-Agent':'video-pipeline-mvp/1.0'}
def run_shot(base,key,shot,poll_seconds=10,deadline=3600):
 body={'model':shot['model'],'prompt':shot['prompt'],'duration':shot.get('duration',5),'resolution':shot.get('resolution','720p'),'aspect_ratio':shot.get('aspect_ratio','16:9')}
 req=Request(base.rstrip('/')+'/videos/generations',data=json.dumps(body).encode(),headers=headers(key))
 with urlopen(req,timeout=90) as r: job=json.load(r)['request_id']
 end=time.monotonic()+deadline
 while time.monotonic()<end:
  with urlopen(Request(base.rstrip('/')+'/videos/'+job,headers=headers(key)),timeout=90) as r: status=json.load(r)
  if status.get('status')=='done': return {'id':shot['id'],'request_id':job,'status':'done'}
  if status.get('status') in ('failed','expired'): return {'id':shot['id'],'request_id':job,'status':status['status']}
  time.sleep(poll_seconds)
 return {'id':shot['id'],'request_id':job,'status':'resume'}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('plan');ap.add_argument('--workers',type=int,default=2);a=ap.parse_args()
 if not 1<=a.workers<=8: ap.error('workers must be 1..8')
 plan=json.load(open(a.plan)); base=os.getenv('VIDEO_API_BASE_URL','https://argolink.io/v1'); key=os.environ['VIDEO_API_KEY']
 with ThreadPoolExecutor(max_workers=a.workers) as pool:
  out=list(pool.map(lambda x:run_shot(base,key,x),plan['shots']))
 print(json.dumps({'shots':out},indent=2))
if __name__=='__main__':main()
