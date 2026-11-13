#!/usr/bin/env python3
"""Validate export completeness, translations, local URLs, kit contents and privacy."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import urlparse,urljoin,unquote
import hashlib,json,re,sys,zipfile
ROOT=Path(__file__).resolve().parents[1];SITE=ROOT/'build/site';OUT=SITE/'distributed-compute-lab'
class Page(HTMLParser):
    def __init__(self):super().__init__();self.links=[];self.ids=set();self.h1=0;self.lang='';self.alt={};self.title=False
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='html':self.lang=a.get('lang','')
        if tag=='h1':self.h1+=1
        if tag=='title':self.title=True
        if 'id' in a:
            if a['id'] in self.ids:raise AssertionError('duplicate id '+a['id'])
            self.ids.add(a['id'])
        for key in ['href','src']:
            if a.get(key):self.links.append(a[key])
        if tag=='link' and a.get('hreflang'):self.alt[a['hreflang']]=a['href']
pages={}
for f in OUT.rglob('*.html'):
    p=Page();p.feed(f.read_text());pages[f]=p
    assert p.title and p.lang
    if f!=OUT/'index.html':
        assert p.h1==1,(f,p.h1)
        assert set(p.alt)=={'en-US','zh-CN','x-default'},f
        assert p.lang==('zh-CN' if '/zh/' in str(f) else 'en-US')
assert len(pages)==25,len(pages)
for f,p in pages.items():
    for link in p.links:
        u=urlparse(urljoin('https://lynnlee1229.github.io/'+str(f.relative_to(SITE)),link))
        if u.netloc!='lynnlee1229.github.io' or u.scheme not in ['http','https']:continue
        if not u.path.startswith('/distributed-compute-lab/'):continue # homepage links verified by whole-site checker
        target=SITE/unquote(u.path).lstrip('/')
        if target.is_dir():target/='index.html'
        assert target.exists(),(f,link)
        if u.fragment and target in pages:assert unquote(u.fragment) in pages[target].ids,(f,link)
manifest=json.loads((OUT/'export-manifest.json').read_text())['files']
assert set(manifest)=={str(f.relative_to(OUT)) for f in OUT.rglob('*') if f.is_file() and f.name!='export-manifest.json'}
for path,sha in manifest.items():assert hashlib.sha256((OUT/path).read_bytes()).hexdigest()==sha,path
for f in OUT.rglob('*'):
    if f.is_file() and f.suffix in ['.json','.html','.js','.css','.py','.xml','.txt','.svg']:
        text=f.read_text()
        assert not re.search(r'/Users/|/home/|/private/|mailto:|tel:|sk-proj-|gh[pous]_[A-Za-z0-9]{20}',text),f
with zipfile.ZipFile(OUT/'downloads/distributed-compute-lab.zip') as z:
    names=z.namelist()
    assert all(n.startswith('distributed-compute-lab/') and '..' not in Path(n).parts for n in names)
    assert not any(set(Path(n).parts)&{'build','.venv','.git','__pycache__','myWebsiteSource'} for n in names)
    assert not any(n.endswith(('.log','.env','.pem','.key')) for n in names)
    assert all('distributed-compute-lab/'+n in names for n in ['experiments/run.py','experiments/aqe.py','scripts/run_all.py','scripts/run_aqe.py','scripts/check_aqe.py','scripts/export_results.py','resources/catalog.json','tests/test_evidence.py','requirements.txt','LICENSE','README.md','VALIDATION.md'])
    for n in names:
        if n.endswith(('.md','.json','.py','.yml','.txt','.js','.css')):
            text=z.read(n).decode()
            # Scanner source may intentionally mention forbidden path/token patterns; actual personal paths are forbidden.
            assert not re.search(r'/Users/[A-Za-z0-9]|/home/[A-Za-z0-9]|/private/(?:tmp|var)/|sk-proj-[A-Za-z0-9]{10}|gh[pous]_[A-Za-z0-9]{20}',text),n
print('PASS: 24 paired content pages + redirect; translations, links, anchors, hashes, source archive and privacy')
