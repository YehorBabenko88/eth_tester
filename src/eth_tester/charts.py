from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .database import connection

def render_trade(settings,trade_id,output_dir="runtime/charts"):
    with connection(settings) as conn:
        trade=conn.execute("SELECT * FROM paper_trades WHERE id=%s",(trade_id,)).fetchone()
        if not trade:
            raise ValueError("Unknown trade")
        rows=conn.execute("""SELECT ts,open,high,low,close FROM candles WHERE symbol=%s AND
           kind='trade' AND tf='5' AND ts BETWEEN %s - interval '12 hours'
           AND coalesce(%s,now()) + interval '1 hour' ORDER BY ts LIMIT 600""",
           (trade["symbol"],trade["opened_at"],trade["closed_at"])).fetchall()
    if not rows:
        raise RuntimeError("No candles available")
    fig,ax=plt.subplots(figsize=(14,6))
    for i,row in enumerate(rows):
        colour="green" if row["close"]>=row["open"] else "red"
        ax.plot([i,i],[float(row["low"]),float(row["high"])],color=colour)
        ax.plot([i,i],[float(row["open"]),float(row["close"])],color=colour,linewidth=4)
    index=min(range(len(rows)),key=lambda i:abs((rows[i]["ts"]-trade["opened_at"]).total_seconds()))
    ax.scatter([index],[float(trade["entry"])],marker="^",label="Entry")
    if trade["closed_at"] and trade["exit"]:
        index=min(range(len(rows)),key=lambda i:abs((rows[i]["ts"]-trade["closed_at"]).total_seconds()))
        ax.scatter([index],[float(trade["exit"])],marker="v",label="Exit")
    ax.set_title(f"{trade['symbol']} paper #{trade_id}: {trade['reason']}")
    ax.legend()
    directory=Path(output_dir)
    directory.mkdir(parents=True,exist_ok=True)
    stage="exit" if trade["closed_at"] else "entry"
    path=directory/f"paper_trade_{trade_id}_{stage}.png"
    fig.savefig(path,dpi=140)
    plt.close(fig)
    with connection(settings) as conn:
        field="exit_chart" if trade["closed_at"] else "entry_chart"
        conn.execute(f"UPDATE paper_trades SET {field}=%s WHERE id=%s",(str(path),trade_id))
    return path


def render_backtest_evidence(candles, deals, output_dir='runtime/backtest_charts', run_id='local'):
    """Produce separate immutable-looking entry/exit PNGs for each scenario trade.

    Source OHLC is explicitly labeled: these are NOT evidence of exchange fills.
    The hosting runner must persist paths and archive/checksum as needed.
    """
    import re

    times = [row['ts'] for row in candles]
    if not times or len(times) != len(set(times)) or times != sorted(times):
        raise ValueError('unique ordered candles required for chart evidence')
    at = {ts: i for i, ts in enumerate(times)}
    safe_run = re.sub(r'[^a-zA-Z0-9_-]', '_', str(run_id))[:80]
    folder = Path(output_dir)
    folder.mkdir(parents=True, exist_ok=True)
    outputs = []
    for number, trade in enumerate(deals, 1):
        if trade['opened_at'] not in at or trade['closed_at'] not in at:
            raise ValueError('trade timestamp not matched to provided candles')
        enter, leave = at[trade['opened_at']], at[trade['closed_at']]
        if enter > leave:
            raise ValueError('exit precedes entry')
        lo = max(0, enter - 30)
        hi = min(len(times), leave + 11)
        view = candles[lo:hi]
        files = {}
        for stage in ('entry', 'exit'):
            fig, ax = plt.subplots(figsize=(13, 6))
            try:
                for index, bar in enumerate(view, lo):
                    up = float(bar['close']) >= float(bar['open'])
                    color = '#26735c' if up else '#b34f50'
                    ax.plot((index, index), (float(bar['low']), float(bar['high'])),
                            color=color, linewidth=1)
                    ax.plot((index, index), (float(bar['open']), float(bar['close'])),
                            color=color, linewidth=4)
                ax.scatter((enter,), (float(trade['entry']),), marker='^' if trade['direction'] == 'long' else 'v',
                           color='#1b5e9a', s=100, zorder=5, label='ENTRY (scenario)')
                if stage == 'exit':
                    ax.scatter((leave,), (float(trade['exit']),), marker='x',
                               color='#bc5829', s=110, zorder=5, label='EXIT (scenario)')
                ax.axhline(float(trade['stop']), linestyle='--', alpha=.65, label='Stop')
                ax.axhline(float(trade['take']), linestyle=':', alpha=.65, label='Take')
                ax.set_title(f"ETHUSDT {trade['direction']} #{number} {stage} | {trade['outcome']}")
                ax.set_xlabel('Candle ordinal (chronological)')
                ax.set_ylabel('Price (USDT)')
                ax.text(.01, .02, 'PAPER / OHLC SCENARIO — NOT VERIFIED EXECUTION',
                        transform=ax.transAxes, fontsize=9, alpha=.75)
                ax.legend(loc='upper right')
                fig.tight_layout()
                location = folder / f'{safe_run}_trade_{number:05d}_{stage}.png'
                fig.savefig(location, dpi=130)
                files[stage + '_chart'] = str(location)
            finally:
                plt.close(fig)
        outputs.append(files)
    return outputs
