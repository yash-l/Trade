from datetime import date
from pathlib import Path
from hydra.costs import EffectiveCostEngine
from hydra.config import Settings
from hydra.risk import RiskEngine
from hydra.models import Quote,Signal
from hydra.timeutil import now_ist

def test_cost_positive():
    c=EffectiveCostEngine().estimate(100,110,65,2,date(2026,9,4)); assert c.total>40; assert c.stt>0

def test_cost_missing_historical_fails_closed():
    try: EffectiveCostEngine().estimate(100,110,65,2,date(2025,1,1)); assert False
    except ValueError as e: assert 'COST_RATE_MISSING' in str(e)

def test_wide_spread_rejected():
    cfg=Settings(); r=RiskEngine(cfg,EffectiveCostEngine()); ts=now_ist(); sig=Signal(ts,'APEX','LONG',90,24000,23980,23990,24040,'test'); q=Quote(ts,'x',1,95,105,100,delta=.6)
    assert not r.can_trade(sig,q).approved

def test_demo_generates_apex_signal_and_trade(tmp_path, monkeypatch):
    import asyncio, os
    from hydra.engine import HydraEngine
    # Settings values are imported defaults, so construct then override DB path via dataclass replacement.
    from dataclasses import replace
    cfg=replace(Settings(), db_path=tmp_path/'hydra.sqlite3')
    async def run():
        e=HydraEngine(cfg)
        for _ in range(90):
            await e.process_candle(e.demo.next_candle())
        s=e.snapshot()
        assert s['last_signal'] is not None
        assert s['trades'] >= 1
        await e.five.close()
    asyncio.run(run())

def test_live_order_is_fail_closed():
    import asyncio
    from hydra.broker.fivepaisa import FivePaisaAdapter, FivePaisaError
    f=FivePaisaAdapter(Settings())
    async def run():
        try:
            await f.place_limit_order(scrip_code=123,qty=65,price=100,side='BUY',remote_order_id='TEST')
            assert False
        except FivePaisaError as e:
            assert 'BLOCKED' in str(e)
        finally:
            await f.close()
    asyncio.run(run())


def _paper_pos(side='LONG'):
    from hydra.models import PaperPosition
    ts=now_ist()
    if side=='LONG':
        return PaperPosition('p',ts,'LONG','x',1,65,100,0,90,120,100,.6,90,120,10)
    return PaperPosition('p',ts,'SHORT','x',1,65,100,0,110,80,100,.6,110,80,10)

def test_trailing_long_ratchets_stop_and_target():
    from hydra.position_manager import PositionManager
    from hydra.models import Candle
    pm=PositionManager(Settings()); p=_paper_pos('LONG'); ts=now_ist()
    # +1.1R activates BE protection.
    u=pm.update(p,Candle(ts,100,112,105,111,1000),None)
    assert p.breakeven_active and p.stop_underlying >= 100
    first_stop=p.stop_underlying
    # +1.6R activates 3m trailing + runner target.
    last3=Candle(ts,110,117,109,116,3000)
    u=pm.update(p,Candle(ts,110,117,110,116,1000),last3)
    assert p.trailing_stop_active and p.trailing_target_active
    assert p.stop_underlying >= first_stop
    assert p.target_underlying > 120
    first_target=p.target_underlying
    # Pullback must never loosen stop or target.
    pm.update(p,Candle(ts,116,116,112,113,1000),Candle(ts,116,116,111,113,3000))
    assert p.stop_underlying >= first_stop
    assert p.target_underlying >= first_target

def test_trailing_short_ratchets_only_down():
    from hydra.position_manager import PositionManager
    from hydra.models import Candle
    pm=PositionManager(Settings()); p=_paper_pos('SHORT'); ts=now_ist()
    pm.update(p,Candle(ts,100,95,83,84,1000),Candle(ts,95,96,83,84,3000))
    assert p.breakeven_active and p.trailing_stop_active and p.trailing_target_active
    assert p.stop_underlying <= 100
    assert p.target_underlying < 80
    stop=p.stop_underlying; target=p.target_underlying
    pm.update(p,Candle(ts,84,90,84,89,1000),Candle(ts,84,90,84,89,3000))
    assert p.stop_underlying <= stop
    assert p.target_underlying <= target

def test_target_is_capped_at_max_r():
    from hydra.position_manager import PositionManager
    from hydra.models import Candle
    pm=PositionManager(Settings()); p=_paper_pos('LONG'); ts=now_ist()
    pm.update(p,Candle(ts,100,180,170,175,1000),Candle(ts,170,180,165,175,3000))
    assert abs(p.target_underlying-(p.underlying_entry+Settings().max_target_r*p.initial_r_points)) < 1e-9


def _scrip_row(code, typ, strike, expiry, lot=65, exch_type='D'):
    return {
        'Exch': 'N', 'ExchType': exch_type, 'SymbolRoot': 'NIFTY',
        'StrikeRate': str(strike), 'ScripType': typ, 'Expiry': expiry,
        'ScripCode': str(code), 'LotSize': str(lot),
        'Name': f'NIFTY {expiry} {typ} {strike}'
    }


def test_5paisa_option_resolver_skips_0_1dte_and_returns_atm_itm():
    import asyncio
    from datetime import date
    from hydra.broker.fivepaisa import FivePaisaAdapter
    from hydra.timeutil import now_ist
    f=FivePaisaAdapter(Settings())
    f._scrip_rows=[
        _scrip_row(1,'CE',24000,'2026-09-08'),
        _scrip_row(2,'CE',23950,'2026-09-08'),
        _scrip_row(11,'CE',24000,'2026-09-15'),
        _scrip_row(12,'CE',23950,'2026-09-15'),
        _scrip_row(21,'PE',24000,'2026-09-15'),
        _scrip_row(22,'PE',24050,'2026-09-15'),
    ]
    f._scrip_loaded_at=now_ist()
    async def run():
        cs=await f.resolve_option_candidates(spot=24012,side='LONG',asof=date(2026,9,7))
        assert [c.scrip_code for c in cs]==[11,12]
        assert all(c.expiry.isoformat()=='2026-09-15' for c in cs)
        await f.close()
    asyncio.run(run())


def test_5paisa_market_depth_parser_uses_best_bid_ask():
    from datetime import date
    from hydra.broker.fivepaisa import FivePaisaAdapter,OptionContract
    c=OptionContract(123,'NIFTY CE', 'CE',24000,date(2026,9,15),65)
    data={'body':{'MarketDepthData':[
        {'BbBuySellFlag':66,'Price':100.0,'Quantity':10},
        {'BbBuySellFlag':66,'Price':100.5,'Quantity':65},
        {'BbBuySellFlag':83,'Price':101.5,'Quantity':65},
        {'BbBuySellFlag':83,'Price':101.0,'Quantity':130},
    ]}}
    q=FivePaisaAdapter.quote_from_market_depth(data,c)
    assert q.bid==100.5 and q.ask==101.0 and q.bid_qty==65 and q.ask_qty==130


def test_live_5paisa_signal_opens_paper_from_realistic_quote(tmp_path):
    import asyncio
    from dataclasses import replace
    from datetime import date
    from hydra.engine import HydraEngine
    from hydra.broker.fivepaisa import OptionContract
    from hydra.models import Quote,Signal
    ts=now_ist()
    cfg=replace(Settings(), db_path=tmp_path/'hydra.sqlite3')
    e=HydraEngine(cfg)
    contract=OptionContract(777,'NIFTY TEST CE','CE',24000,date(2026,9,15),65)
    q=Quote(ts,'NIFTY TEST CE',777,50.0,50.1,50.05,bid_qty=500,ask_qty=500,strike=24000,option_type='CE',expiry='2026-09-15')
    sig=Signal(ts,'APEX','LONG',90,24000,23990,23998,24004,'test')
    async def fake_best(**kwargs): return contract,q,[{'viable':True}]
    async def fake_start(contract): e.selected_contract=contract
    e.five.best_executable_option=fake_best
    e._start_option_stream=fake_start
    async def run():
        await e._handle_5paisa_signal(sig)
        assert e.paper.position is not None
        assert e.paper.position.scrip_code==777
        assert e.paper.position.entry_price==50.1
        await e.five.close()
    asyncio.run(run())

from hydra.execution.order_fsm import OrderRecord, OrderState, state_from_confirmation
from hydra.research_engine.validation import cpcv_splits, purged_embargo_splits, monte_carlo, profit_concentration
from hydra.research_engine.drift import rolling_edge_drift

def test_order_fsm_and_confirmation():
    r=OrderRecord('RID1','BUY',65,100.0)
    r.transition(OrderState.SUBMITTED); r.transition(OrderState.PLACED)
    assert state_from_confirmation({'ReqType':'T','Status':'Fully Executed','PendingQty':0})==OrderState.FILLED

def test_research_splits_and_mc():
    folds=list(purged_embargo_splits(100,5,purge=2,embargo=2)); assert len(folds)==5
    cp=list(cpcv_splits(120,6,2,purge=2,embargo=2)); assert len(cp)==15
    assert monte_carlo([1,-1,2,-.5],10000)['iterations']==10000

def test_research_concentration_and_drift():
    c=profit_concentration([10,5,-1,1]); assert c['top1_share']>0.5
    assert rolling_edge_drift([1.0]*60,30)['degraded'] is False


def test_adaptive_features_and_outcome_events(tmp_path):
    import asyncio
    from dataclasses import replace
    from hydra.engine import HydraEngine
    cfg=replace(Settings(), db_path=tmp_path/'adaptive.sqlite3')
    async def run():
        e=HydraEngine(cfg)
        for _ in range(90): await e.process_candle(e.demo.next_candle())
        kinds=[x['kind'] for x in e.journal.events(500)]
        assert 'SIGNAL_FEATURES' in kinds
        assert 'TRADE_OUTCOME' in kinds
        await e.five.close()
    asyncio.run(run())

def test_adaptive_bounds(tmp_path):
    from dataclasses import replace
    from hydra.journal import Journal
    from hydra.adaptive import AdaptiveLayer
    cfg=replace(Settings(), db_path=tmp_path/'a.sqlite3')
    a=AdaptiveLayer(cfg,Journal(cfg.db_path))
    x=a.trail_params('LONG|LONG|OPEN|85-89')
    assert 0.05 <= x['buffer_r'] <= 0.20
    assert 0.50 <= x['breakeven_trigger_r'] <= 1.50

def test_shadow_model_requires_sample_and_bounds():
    from hydra.research_engine.adaptive_model import ShadowEntryModel, trailing_param_sweep
    assert ShadowEntryModel.fit([{'score':90,'rvol':1,'atr':20,'r_distance':1,'spread_pct':0.3,'outcome_net':1}]*49) is None
    rows=[]
    for i in range(60): rows.append({'score':85+i%10,'rvol':1.2,'atr':20,'r_distance':1,'spread_pct':0.3,'outcome_net':1 if i%2 else -1})
    m=ShadowEntryModel.fit(rows); assert m is not None
    assert 0<=m.predict_proba(rows[0])<=1
    sw=trailing_param_sweep([{'highest_r':2,'captured_r':1,'reason':'TRAIL_STOP'}]*60)
    assert sw['status']=='SHADOW_ONLY' and sw['best']['buffer_r'] in {0.05,0.10,0.15,0.20}

def test_journal_hash_chain_detects_tamper(tmp_path):
    from hydra.journal import Journal
    j=Journal(tmp_path/'j.sqlite3')
    j.append('A',{'x':1}); j.append('B',{'y':2})
    assert j.verify_chain()['valid'] is True
    import sqlite3
    con=sqlite3.connect(tmp_path/'j.sqlite3'); con.execute("update events set payload='{}' where id=1"); con.commit(); con.close()
    assert j.verify_chain()['valid'] is False


def test_adaptive_entry_tightens_only_after_loss_count(tmp_path):
    from hydra.journal import Journal
    from hydra.adaptive import AdaptiveLayer
    from dataclasses import replace
    cfg=replace(Settings(), db_path=tmp_path/'a.sqlite3', adaptive_shadow_only=True)
    j=Journal(cfg.db_path); a=AdaptiveLayer(cfg,j); bucket='LONG|LONG|OPEN|85-89'
    for i in range(2): j.append('TRADE_OUTCOME',{'bucket_key':bucket,'net':-10})
    assert a.entry_decision(bucket,85)['threshold']==85
    j.append('TRADE_OUTCOME',{'bucket_key':bucket,'net':-10})
    d=a.entry_decision(bucket,85); assert d['threshold']==95 and d['adjusted'] is True
    j.append('TRADE_OUTCOME',{'bucket_key':bucket,'net':-10})
    assert a.entry_decision(bucket,85)['threshold']==95


def test_adaptive_trailing_adjustments_are_bounded(tmp_path):
    from hydra.journal import Journal
    from hydra.adaptive import AdaptiveLayer
    from dataclasses import replace
    cfg=replace(Settings(), db_path=tmp_path/'a.sqlite3', adaptive_min_shadow_trades=50)
    j=Journal(cfg.db_path); a=AdaptiveLayer(cfg,j); bucket='LONG|LONG|OPEN|85-89'
    for _ in range(50):
        j.append('TRADE_OUTCOME',{'bucket_key':bucket,'net':-1,'reason':'TRAIL_STOP','highest_r':2.0,'captured_r':0.5})
    x=a.trail_params(bucket)
    assert x['changed'] is True and 0.05 <= x['buffer_r'] <= 0.20


def test_shadow_model_uses_training_normalization_at_prediction():
    from hydra.research_engine.adaptive_model import ShadowEntryModel
    rows=[]
    for i in range(60):
        rows.append({'score':85+i%10,'rvol':1+i*0.01,'atr':20+i%3,'r_distance':1+i%2,'spread_pct':0.2+i%5*0.01,'outcome_net':1 if i%2 else -1})
    m=ShadowEntryModel.fit(rows)
    assert m is not None
    p1=m.predict_proba(rows[0]); p2=m.predict_proba(rows[-1])
    assert 0 <= p1 <= 1 and 0 <= p2 <= 1 and p1 != p2


def test_cross_validated_shadow_is_purged_and_shadow_only():
    from hydra.research_engine.adaptive_model import cross_validated_shadow
    rows=[]
    for i in range(100):
        rows.append({'score':85+i%10,'rvol':1.1,'atr':20,'r_distance':1,'spread_pct':0.3,'outcome_net':1 if i%3 else -1})
    r=cross_validated_shadow(rows,folds=5,purge=2,embargo=2)
    assert r['status']=='SHADOW_ONLY' and r['folds'] >= 1 and 0 <= r['mean_accuracy'] <= 1


def test_training_export_exact_signal_join(tmp_path):
    import subprocess, sys, csv
    from hydra.journal import Journal
    db=tmp_path/'e.sqlite3'; j=Journal(db)
    sid='s1'; pid='p1'
    j.append('SIGNAL_FEATURES',{'signal_id':sid,'bucket_key':'B','rvol':1.2})
    j.trade({'id':pid,'opened_at':'2026-09-14T09:30:00+05:30','closed_at':'2026-09-14T09:35:00+05:30','symbol':'X','qty':65,'entry':100,'exit':101,'gross':65,'costs':10,'net':55,'side':'LONG','reason':'TARGET'})
    j.append('TRADE_OUTCOME',{'position_id':pid,'signal_id':sid,'bucket_key':'B','net':55,'highest_r':2,'captured_r':1.8,'reason':'TARGET'})
    out=tmp_path/'training.csv'
    subprocess.run([sys.executable,'scripts/export_training_data.py','--db',str(db),'--out',str(out)],check=True,cwd=Path(__file__).resolve().parents[1])
    rows=list(csv.DictReader(out.open()))
    assert len(rows)==1 and rows[0]['rvol']=='1.2' and rows[0]['outcome_net']=='55'


def test_adaptive_trail_shadow_does_not_modify_paper_position(tmp_path):
    from dataclasses import replace
    from hydra.engine import HydraEngine
    from hydra.models import Quote, Signal
    cfg=replace(Settings(), db_path=tmp_path/'shadow.sqlite3')
    e=HydraEngine(cfg)
    ts=now_ist()
    sig=Signal(ts,'APEX','LONG',90,24000,23980,23990,24040,'test',adaptive_bucket='LONG|LONG|OPEN|85-89')
    q=Quote(ts,'NIFTY TEST CE',123,100,101,100.5,delta=.6)
    trail={'changed':True,'buffer_r':0.20,'breakeven_trigger_r':0.50,'bucket_key':sig.adaptive_bucket}
    e.adaptive.trail_params=lambda bucket: trail
    applied=None if e.cfg.adaptive_trail_shadow_only else trail
    p=e.paper.open(sig,q,applied)
    assert p.adaptive_trailing_stop_buffer_r==cfg.trailing_stop_buffer_r
    assert p.adaptive_breakeven_trigger_r==cfg.breakeven_trigger_r

def test_adaptive_trail_applies_only_when_shadow_disabled(tmp_path):
    from dataclasses import replace
    from hydra.engine import HydraEngine
    from hydra.models import Quote, Signal
    cfg=replace(Settings(), db_path=tmp_path/'apply.sqlite3')
    e=HydraEngine(cfg)
    ts=now_ist()
    sig=Signal(ts,'APEX','LONG',90,24000,23980,23990,24040,'test',adaptive_bucket='LONG|LONG|OPEN|85-89')
    q=Quote(ts,'NIFTY TEST CE',123,100,101,100.5,delta=.6)
    trail={'changed':True,'buffer_r':0.20,'breakeven_trigger_r':0.50,'bucket_key':sig.adaptive_bucket}
    applied_trail=trail
    p=e.paper.open(sig,q,applied_trail)
    assert p.adaptive_trailing_stop_buffer_r==0.20
    assert p.adaptive_breakeven_trigger_r==0.50

def test_demo_reset_does_not_orphan_position(tmp_path):
    import asyncio
    from dataclasses import replace
    from hydra.engine import HydraEngine
    from hydra.models import Quote, Signal
    cfg=replace(Settings(), db_path=tmp_path/'reset.sqlite3')
    async def run():
        e=HydraEngine(cfg)
        ts=now_ist()
        sig=Signal(ts,'APEX','LONG',90,24000,23980,23990,24040,'test',adaptive_bucket='LONG|LONG|OPEN|85-89')
        q=Quote(ts,'DEMO CE',777,100,101,100.5,delta=.6)
        e.paper.open(sig,q)
        e.latest_quote=q
        reset_q=await e._paper_quote_for_position(24000)
        assert reset_q is not None
        row=e.paper.close(reset_q,24000,'DEMO_RESET',ts)
        assert row['reason']=='DEMO_RESET' and e.paper.position is None
    asyncio.run(run())

def test_time_exit_after_1510_closes_position(tmp_path):
    import asyncio
    from dataclasses import replace
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from hydra.engine import HydraEngine
    from hydra.models import Candle, Quote, Signal
    cfg=replace(Settings(), db_path=tmp_path/'timeexit.sqlite3')
    async def run():
        e=HydraEngine(cfg)
        ts=datetime(2026,9,14,15,11,tzinfo=ZoneInfo('Asia/Kolkata'))
        sig=Signal(ts,'APEX','LONG',90,24000,23980,23990,24040,'test',adaptive_bucket='LONG|LONG|OPEN|85-89')
        q=Quote(ts,'TEST CE',777,100,101,100.5,delta=.6)
        e.paper.open(sig,q)
        async def fixed_quote(underlying):
            return q
        e._paper_quote_for_position=fixed_quote
        c=Candle(ts,24000,24002,23999,24000,1000)
        await e._manage_open_position(c)
        assert e.paper.position is None
        rows=e.journal.trades(10)
        assert rows and rows[-1]['reason']=='TIME_EXIT'
    asyncio.run(run())


def test_adaptive_snapshot_exposes_ui_data_and_filters_events(tmp_path):
    from dataclasses import replace
    from hydra.engine import HydraEngine
    cfg=replace(Settings(), db_path=tmp_path/'adaptive_ui.sqlite3')
    e=HydraEngine(cfg)
    bucket='LONG|LONG|OPEN|85-89'
    e.journal.append('TRADE_OUTCOME',{'bucket_key':bucket,'net':-10,'highest_r':1.8,'captured_r':0.4,'reason':'TRAIL_STOP'})
    e.journal.append('ADAPTIVE_PARAM_ADJUSTED_SHADOW',{'bucket_key':bucket,'scope':'TRAIL'})
    e.journal.append('AI_GATE_SHADOW',{'bucket_key':bucket,'action':'WOULD_SKIP'})
    e.journal.append('PAPER_CLOSE',{'bucket_key':bucket})
    a=e.adaptive_snapshot()
    assert a['shadow_mode'] is True and a['labeled_trades']==1
    assert a['buckets'][0]['key']==bucket and a['buckets'][0]['adjustments']==1
    assert {x['kind'] for x in a['events']}=={'ADAPTIVE_PARAM_ADJUSTED_SHADOW','AI_GATE_SHADOW'}
