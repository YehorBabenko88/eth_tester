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
    path=directory/f"paper_trade_{trade_id}.png"
    fig.savefig(path,dpi=140)
    plt.close(fig)
    with connection(settings) as conn:
        conn.execute("UPDATE paper_trades SET exit_chart=%s WHERE id=%s",(str(path),trade_id))
    return path
