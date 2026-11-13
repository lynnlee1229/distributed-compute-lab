#!/usr/bin/env python3
"""Dependency-free bilingual static export. Only allowlisted files enter the kit."""
from pathlib import Path
from html import escape
import hashlib,json,re,shutil,zipfile
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'build/site/distributed-compute-lab'
BASE='/distributed-compute-lab/'
ORIGIN='https://lynnlee1229.github.io'
REPO='https://github.com/lynnlee1229/distributed-compute-lab'
CHAPTERS=[('01-parallel','Why parallelize?','为什么并行？','Work, slots, overhead and correctness','工作量、槽位、开销与正确性'),
 ('02-model','Data-parallel models','数据并行模型','MapReduce, dependencies, stages and tasks','MapReduce、依赖、阶段与任务'),
 ('03-aggregation','Aggregate before moving','先聚合，再移动','Mergeable summaries and Shuffle evidence','可合并摘要与 Shuffle 证据'),
 ('04-join','Join strategies and skew','Join 策略与倾斜','Broadcast, redistribution and salting tradeoffs','广播、重分布与加盐取舍'),
 ('05-recovery','Retry and recompute','重试与重算','Lineage, cache and checkpoint boundaries','Lineage、缓存与检查点边界'),
 ('06-resources','Resource diagnosis','资源诊断','Spill, task spread and performance reasoning','Spill、任务分布与性能推理')]

def choose(lang,en,zh):return zh if lang=='zh' else en

def inline(s):
    s=escape(s)
    s=re.sub(r'`([^`]+)`',r'<code>\1</code>',s)
    s=re.sub(r'\[([^\]]+)\]\((https?://[^ )]+|[^ )]+)\)',lambda m:f'<a href="{m[2]}">{m[1]}</a>',s)
    return s

def flow(lang,kind):
    labels={
     'dag': [('Input partitions','输入分区','blue 2 / red 3 · blue 5 / red 1'),('Stage A · map','Stage A · map','independent / 独立变换'),('Shuffle','Shuffle','key → owner / 键 → 归属'),('Stage B · reduce','Stage B · reduce','blue 7 · red 4')],
     'aggregate':[('Partition 0','分区 0','2 + 4 → (6,2)'),('Partition 1','分区 1','9 → (9,1)'),('Merge','合并','(15,3) → mean 5')],
     'join':[('Broadcast','广播','small → each task / 小表 → 各任务'),('Shuffle Join','Shuffle Join','same key → same owner / 同键同归属'),('Salt','加盐','(key,salt) + replicated small side / 小表复制')],
     'recovery':[('Recipe','配方','input → transform / 输入 → 变换'),('Cache','缓存','reuse; may disappear / 复用；可能丢失'),('Checkpoint','检查点','materialized boundary / 物化恢复边界')],
     'resources':[('Execution','执行','sort / aggregate buffers'),('Storage','存储','cached blocks'),('Spill','溢写','disk runs → merge')]}
    if kind=='cost':
        return f'''<section class="calculator" id="cost-model" aria-label="{choose(lang,'Illustrative cost model','示意成本模型')}"><span class="eyebrow">{choose(lang,'MODEL · NOT MEASURED','模型 · 非实测')}</span><p>{choose(lang,'800 ms useful work + 200 ms serial preparation; 5 ms overhead per task.','800 ms 有效工作 + 200 ms 串行准备；每任务 5 ms 开销。')}</p><label for="slots">{choose(lang,'Execution slots','执行槽位')} <b id="slots-value">2</b></label><input type="range" id="slots" min="1" max="16" value="2"><label for="parts">{choose(lang,'Equal-sized tasks','等长任务数')} <b id="parts-value">8</b></label><input type="range" id="parts" min="1" max="128" value="8"><output id="model-output" aria-live="polite"></output><p class="small" id="model-detail"></p><noscript>{choose(lang,'For eight tasks and two slots: 200 + 4 × (100 + 5) = 620 ms.','八任务、两槽位：200 + 4 × (100 + 5) = 620 ms。')}</noscript></section>'''
    nodes=''.join(f'<div class="node {"shuffle" if "Shuffle" in en else ""}"><b>{choose(lang,en,cn)}</b>{escape(detail)}</div>' for en,cn,detail in labels[kind])
    return '<div class="flow">'+nodes+'</div><p class="flow-caption">'+choose(lang,'Original conceptual diagram · not an execution trace.','原创概念图 · 不是运行轨迹。')+'</p>'

def markdown(text,lang):
    lines=text.splitlines();out=[];paragraph=[];code=None;listing=False;quiz=False
    def flush():
        if paragraph:out.append('<p>'+inline(' '.join(paragraph))+'</p>');paragraph.clear()
    def close_list():
        nonlocal listing
        if listing:out.append('</ul>');listing=False
    def close_quiz():
        nonlocal quiz
        if quiz:out.append('</details>');quiz=False
    for line in lines:
        if line.startswith('```'):
            flush();close_list();close_quiz()
            if code is None:code=[]
            else:out.append('<pre><code>'+escape('\n'.join(code))+'</code></pre>');code=None
            continue
        if code is not None:code.append(line);continue
        if not line.strip():flush();close_list();continue
        if line.startswith(':::'):flush();close_list();close_quiz();out.append(flow(lang,line[3:]));continue
        if line.startswith('? '):flush();close_list();close_quiz();out.append('<details class="quiz"><summary>'+inline(line[2:])+'</summary>');quiz=True;continue
        if line.startswith('! '):flush();out.append('<p>'+inline(line[2:])+'</p>');continue
        if line.startswith('#'):
            flush();close_list();close_quiz();level=len(line)-len(line.lstrip('#'));title=line[level:].strip();slug=re.sub(r'[^a-z0-9]+','-',title.lower()).strip('-') or 'section-'+str(len(out))
            out.append(f'<h{level} id="{slug}">{inline(title)}</h{level}>');continue
        if line.startswith('- '):
            flush();close_quiz()
            if not listing:out.append('<ul>');listing=True
            out.append('<li>'+inline(line[2:])+'</li>');continue
        close_quiz();paragraph.append(line)
    flush();close_list();close_quiz();return '\n'.join(out)

def page(lang,slug,title,body,desc=''):
    other='zh' if lang=='en' else 'en';path=BASE+lang+'/'+(slug+'/' if slug else '')
    paired=BASE+other+'/'+(slug+'/' if slug else '')
    langtag='en-US' if lang=='en' else 'zh-CN'
    links=[('',choose(lang,'Course overview','课程概览'))]+[(x[0],x[1] if lang=='en' else x[2]) for x in CHAPTERS]+[('evidence',choose(lang,'Recorded evidence','实测证据')),('reproduce',choose(lang,'Reproduce locally','本地复现')),('code',choose(lang,'Read the code','阅读代码'))]
    nav=''.join(f'<a href="{BASE}{lang}/{key+"/" if key else ""}"'+(' aria-current="page"' if key==slug else '')+'>'+escape(name)+'</a>' for key,name in links)
    doc=f'''<!doctype html><html lang="{langtag}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)} · Distributed Compute Lab</title><meta name="description" content="{escape(desc or choose(lang,'Learn distributed computation through original explanations, real Spark experiments and reproducible evidence.','通过原创讲解、真实 Spark 实验与可复现证据学习分布式计算。'))}"><link rel="canonical" href="{ORIGIN}{path}"><link rel="alternate" hreflang="en-US" href="{ORIGIN}{BASE}en/{slug+'/' if slug else ''}"><link rel="alternate" hreflang="zh-CN" href="{ORIGIN}{BASE}zh/{slug+'/' if slug else ''}"><link rel="alternate" hreflang="x-default" href="{ORIGIN}{BASE}en/{slug+'/' if slug else ''}"><link rel="stylesheet" href="{BASE}assets/site.css"><link rel="icon" href="{BASE}assets/favicon.svg"><script defer src="{BASE}assets/site.js"></script></head><body><a class="skip" href="#main">{choose(lang,'Skip to content','跳到正文')}</a><header><div class="top"><a class="brand" href="{BASE}{lang}/"><span>▦</span> Distributed Compute Lab</a><nav aria-label="{choose(lang,'Global','全局导航')}"><a class="home-link" href="{('/zh/' if lang=='zh' else '/')}websites/">Lynn Lee</a><a href="{REPO}">{choose(lang,'Source','源码')}</a><a lang="{'zh-CN' if other=='zh' else 'en-US'}" href="{paired}">{'中文' if lang=='en' else 'English'}</a></nav></div></header><div class="layout"><aside class="sidebar" aria-label="{choose(lang,'Course navigation','课程导航')}"><span class="eyebrow">{choose(lang,'Learning sequence','学习顺序')}</span>{nav}</aside><main id="main">{body}</main></div><footer>Lynn Lee · {choose(lang,'Original course · MIT companion · Recorded on 13 Nov 2026 · No analytics','原创课程 · MIT 配套项目 · 实测日期 2026-11-13 · 无分析追踪')}<br>{choose(lang,'Single-host Spark local mode. Concept diagrams and model estimates are labeled separately.','单机 Spark 本地模式。概念图和模型估算均独立标注。')}</footer></body></html>'''
    dest=OUT/lang/slug/'index.html';dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(doc)

if OUT.exists():shutil.rmtree(OUT)
(OUT/'assets').mkdir(parents=True)
for file in (ROOT/'site').glob('*'):
    if file.is_file():shutil.copy2(file,OUT/'assets'/file.name)
(OUT/'assets/favicon.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="6" fill="#172c36"/><g fill="#9bd8bc"><path d="M6 6h8v8H6zM18 6h8v8h-8zM6 18h8v8H6zM18 18h8v8h-8z"/></g></svg>')
shutil.copytree(ROOT/'results',OUT/'results')
(OUT/'code').mkdir();shutil.copy2(ROOT/'experiments/run.py',OUT/'code/run.py')
code=(ROOT/'experiments/run.py').read_text();codehash=hashlib.sha256(code.encode()).hexdigest()
for lang in ['en','zh']:
    t=lambda en,zh:choose(lang,en,zh)
    cards=''.join(f'<a class="card" href="{BASE}{lang}/{slug}/"><span class="eyebrow">{i:02d} / CORE</span><h3>{t(en,cn)}</h3><p>{t(de,dc)}</p></a>' for i,(slug,en,cn,de,dc) in enumerate(CHAPTERS,1))
    body=f'''<section class="hero"><div><span class="badge">{t('Course + reproducible experiments','课程 + 可复现实验')}</span><h1>{t('Think in work,<br>data movement<br>and recovery.','从工作量、<br>数据移动与<br>恢复理解计算。')}</h1><p class="intro">{t('A foundation-first course in distributed computation. Read the ideas, predict a result, inspect real Spark evidence, then change the experiment.','以基础原理为主线的分布式计算课程。先理解与预测，再检查真实 Spark 证据，最后修改实验。')}</p><div class="actions"><a class="button" href="{BASE}{lang}/01-parallel/">{t('Start the course →','开始学习 →')}</a><a class="button secondary" href="{BASE}{lang}/evidence/">{t('Inspect evidence','查看实测')}</a></div></div><aside class="facts"><span class="eyebrow">{t('Executed baseline','已执行基线')}</span><div><strong>5 {t('experiments','类实验')}</strong><p>{t('Parallelism · aggregation · Join · recovery · resources','并行 · 聚合 · Join · 容错 · 资源')}</p></div><div><strong>240,000 {t('rows','行')}</strong><p>Spark 3.5.8 · JDK 11 · Python 3.11<br>local[2,2] · 1 GiB heap</p></div><div><strong>3 {t('repetitions','次重复')}</strong><p>{t('All observations retained, including variation and unsuccessful tradeoffs.','保留全部观测、波动与没有改善的取舍。')}</p></div><div><strong>1 {t('host','台主机')}</strong><p>{t('Local tasks and Shuffle; no multi-host scaling claim.','本地任务与 Shuffle；不作多机扩展性结论。')}</p></div></aside></section><h2>{t('Enter at the right depth','选择合适的入口')}</h2><div class="note">{t('New to distributed computing? Know loops, functions, key/value pairs and basic sums; read Setup, then Chapters 01–06 in order. Already use an engine? Begin with the prediction and exit task of each chapter, inspect the saved plan, then reproduce before skipping ahead. No domain-specific experience is required.','初学者只需循环、函数、键值对与基本求和知识；先读复现页，再按 01–06 学习。已经使用计算引擎的读者，可先做每章预测和过关任务，检查计划并复现后再决定跳读。不需要特定业务领域经验。')}</div><h2>{t('The core learning path','核心学习路线')}</h2><div class="cards">{cards}</div><h2>{t('One complete learning loop','一个完整的学习闭环')}</h2><p>{t('Explain the invariant → predict the communication → run the code → inspect tasks and plans → change one factor → defend the conclusion. Every chapter has a runnable example, hands-on task, interpretation and self-check answers.','解释不变量 → 预测通信 → 执行代码 → 检查任务与计划 → 改变一个因素 → 用证据支持结论。每章均有可运行示例、动手任务、结果解释和自测答案。')}</p><div class="actions"><a class="button secondary" href="{BASE}{lang}/reproduce/">{t('Reproduce locally','本地复现')}</a><a class="button secondary" download href="{BASE}downloads/distributed-compute-lab.zip">{t('Download the companion','下载配套项目')}</a></div><p class="small">{t('Original explanations with upstream papers and official references. Finite batch fundamentals; advanced distributed algorithms, multi-host scaling and production capacity planning are outside this edition.','原创讲解，引用上游论文与官方资料。首版覆盖有界批计算基础，不覆盖高阶分布式算法、多机扩展和生产容量规划。')}</p>'''
    page(lang,'',t('Course overview','课程概览'),body)
    for i,(slug,en,cn,_,__) in enumerate(CHAPTERS):
        body='<article class="prose">'+markdown((ROOT/f'docs/{slug}.{lang}.md').read_text(),lang)+'</article>'
        body+=f'<button class="read-toggle" id="mark-read" data-chapter="{slug}" aria-pressed="false">{t("Mark chapter as read","标记本章已读")}</button><p class="small">{t("Reading progress is stored only in this browser when available.","阅读标记只保存在当前浏览器（可用时）。")}</p>'
        prev=CHAPTERS[i-1][0] if i else '';nxt=CHAPTERS[i+1][0] if i+1<len(CHAPTERS) else 'evidence'
        body+=f'<nav class="next" aria-label="{t("Chapter navigation","章节导航")}"><a href="{BASE}{lang}/{prev+"/" if prev else ""}">← {t("Previous","上一页")}</a><a href="{BASE}{lang}/{nxt}/">{t("Next","下一页")} →</a></nav>'
        page(lang,slug,t(en,cn),body)
    page(lang,'reproduce',t('Reproduce locally','本地复现'),'<article class="prose">'+markdown((ROOT/f'docs/reproduce.{lang}.md').read_text(),lang)+'</article>')
    options=''.join(f'<option value="{key}">{label}</option>' for key,label in [('aggregation',t('Aggregation / data movement','聚合 / 数据移动')),('join',t('Join / skew','Join / 倾斜')),('recovery',t('Retry / recomputation','重试 / 重算')),('resources',t('Resources / Spill','资源 / Spill')),('parallel',t('Parallelism / overhead','并行 / 开销'))])
    intro=t('Real Spark executions recorded on one host. These tables are not a simulator. Use the controls to inspect saved repetitions, then reproduce the same code locally.','一台主机上真实 Spark 执行的保存结果。这些表格不是模拟器。通过控件检查重复运行，再在本地执行同一份代码。')
    body=f'''<span class="eyebrow">{t('Measured · 13 Nov 2026','实测 · 2026-11-13')}</span><h1>{t('Recorded evidence','实测证据')}</h1><p class="intro">{intro}</p><div class="note">{t('Performance probes: one warm-up per variant, then three measured repeats in alternating order. Recovery: three mechanism traces without warm-up. Timed variants include plan construction, execution and Python/JVM transfer; startup and source-cache materialization are excluded. Resource Spill is intentionally threshold-triggered.','性能探针：每方案一次预热，再交替顺序计时三次。容错：三个不预热的机制轨迹。方案计时包括计划构建、执行与 Python/JVM 传输，不含应用启动和共同输入的缓存物化。资源 Spill 由受控阈值触发。')}</div><section id="evidence-viewer"><div class="controls"><label for="experiment">{t('Experiment','实验')}</label><select id="experiment">{options}</select><label><input type="checkbox" id="warmup"> {t('Show warm-up','显示预热')}</label><a id="download-result" download href="{BASE}results/aggregation.json">{t('Download JSON','下载 JSON')}</a></div><p role="status" id="status"></p><div id="summary" class="metric-grid"></div><p class="small">{t('Cards show median and min–max wall time. Each row below is an observation, not a production recommendation.','卡片展示中位与最小–最大墙钟时间。下表每行是一次观测，不是生产建议。')}</p><div id="runs-table" class="table-wrap"></div><h2>{t('Inspect a run','检查一次运行')}</h2><div class="controls"><label for="run-select">{t('Run group','运行分组')}</label><select id="run-select"></select></div><pre id="run-result"></pre><div id="histogram"></div><details id="task-details"><summary>{t('Task attempts and per-task metrics','任务尝试与逐任务指标')}</summary><div id="task-table" class="table-wrap"></div></details><details id="raw-plan"><summary>{t('Saved lineage / physical plans','保存的 Lineage / 物理计划')}</summary><pre id="plans"></pre></details><details><summary>{t('Conditions and mechanism observations','条件与机制观测')}</summary><pre id="environment"></pre></details><noscript><p>{t('JavaScript is required for this viewer. Every report remains available as plain JSON below.','浏览器查看器需要 JavaScript，下方提供完整 JSON。')}</p></noscript></section><h2>{t('Evidence files and method','证据文件与方法')}</h2><p>{t('Task counters are extracted from Spark TaskEnd events. Failed attempts are retained. Shuffle read bytes include local and remote reads; all remote reads in this local baseline are zero. Shuffle write records can represent batched serialized objects, especially in PySpark. Peak execution memory is a maximum per task, not process RSS.','任务计数来自 Spark TaskEnd 事件，保留失败尝试。Shuffle 读取包括本地和远程；本基线远程读取均为零。Shuffle Record 可能是批量序列化对象，尤其在 PySpark 中。峰值执行内存为逐任务最大值，不是进程 RSS。')}</p><ul>{''.join(f'<li><a href="{BASE}results/{key}.json" download>{key}.json</a></li>' for key in ['parallel','aggregation','join','recovery','resources'])}<li><a href="{BASE}results/index.json">SHA-256 manifest</a></li></ul><p>{t('Raw environment events and runtime logs are excluded; reports contain only allowlisted metrics and path-cleaned plans. Wall times vary with JVM warm-up, OS scheduling and other host activity. No confidence interval or cross-machine generalization is claimed.','原始环境事件与运行日志不分发；报告只含允许字段和清理路径后的计划。JVM 预热、系统调度和其他主机活动会影响耗时。本版不声称统计置信区间，也不推广为多机结论。')}</p><div class="actions"><a class="button" href="{BASE}{lang}/reproduce/">{t('Reproduce these reports','复现这些报告')}</a><a class="button secondary" href="{BASE}{lang}/code/">{t('Read executable source','阅读可执行源码')}</a></div>'''
    page(lang,'evidence',t('Recorded evidence','实测证据'),body)
    body=f'''<span class="eyebrow">{t('Executable source','可执行源码')}</span><h1>{t('Read the experiment code','阅读实验代码')}</h1><p>{t('This is the exact experiment runner used for the saved baseline. The companion also includes the orchestration, sanitizer, tests and data formulas. Read the setup before running: it fixes the runtime and isolates inherited configuration.','这是生成保存基线的实际实验代码。配套项目还包含编排、清理导出、测试与数据公式。执行前请阅读复现页，以固定运行时并隔离继承配置。')}</p><div class="actions"><a class="button" href="{REPO}">{t('Open public repository','打开公开仓库')}</a><a class="button secondary" href="{BASE}downloads/distributed-compute-lab.zip" download>{t('Download full companion','下载完整配套项目')}</a><a class="button secondary" href="{BASE}code/run.py" download>run.py</a></div><p class="code-meta">experiments/run.py · SHA-256 {codehash}</p><pre><code>{escape(code)}</code></pre>'''
    page(lang,'code',t('Read the code','阅读代码'),body)
(OUT/'index.html').write_text('<!doctype html><html lang="en-US"><head><meta charset="utf-8"><title>Distributed Compute Lab</title><meta http-equiv="refresh" content="0;url='+BASE+'en/"><link rel="canonical" href="'+ORIGIN+BASE+'en/"></head><body><a href="'+BASE+'en/">English</a> · <a href="'+BASE+'zh/">中文</a></body></html>')
# A portable source kit, never a workspace snapshot.
files=[]
for folder in ['experiments','scripts','tests','docs','site','results','.github']:
    files.extend(p for p in (ROOT/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc')
files += [ROOT/x for x in ['README.md','README.zh-CN.md','LICENSE','requirements.txt','requirements-dev.txt','.gitignore','VALIDATION.md'] if (ROOT/x).exists()]
(OUT/'downloads').mkdir()
with zipfile.ZipFile(OUT/'downloads/distributed-compute-lab.zip','w',zipfile.ZIP_DEFLATED) as z:
    for f in sorted(set(files)):
        name='distributed-compute-lab/'+str(f.relative_to(ROOT));info=zipfile.ZipInfo(name,(2026,11,13,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,f.read_bytes())
urls=[ORIGIN+BASE+lang+'/'+(slug+'/' if slug else '') for lang in ['en','zh'] for slug in ['',*[c[0] for c in CHAPTERS],'evidence','reproduce','code']]
(OUT/'sitemap.xml').write_text('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'+''.join('<url><loc>'+u+'</loc></url>' for u in urls)+'</urlset>')
(OUT/'robots.txt').write_text('User-agent: *\nAllow: /\nSitemap: '+ORIGIN+BASE+'sitemap.xml\n')
manifest={str(f.relative_to(OUT)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(OUT.rglob('*')) if f.is_file()}
(OUT/'export-manifest.json').write_text(json.dumps({'schema_version':1,'files':manifest},indent=2)+'\n')
print('BUILT',len(urls),'bilingual pages; allowlisted source archive and',len(manifest),'audited assets')
