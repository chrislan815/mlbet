import { useEffect, useRef, useState } from "react"
import { Routes, Route, Link, useParams } from "react-router-dom"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import type { GameInfo, GameState, Pitch, Runners, Count, Play, LinescoreInning, OddsData, Market, OrderBook, OrderBookLevel, Portfolio, Position, Activity } from "./types"

const PITCH_COLORS: Record<string, string> = {
  FF: "#ef4444", SI: "#f97316", FC: "#eab308", SL: "#facc15",
  CU: "#22c55e", KC: "#14b8a6", CH: "#3b82f6", FS: "#a855f7",
  KN: "#6b7280", ST: "#ec4899", SV: "#ec4899", CS: "#10b981",
}

const LIVE = new Set(["In Progress", "Manager Challenge", "Umpire Review", "Delayed", "Warmup", "Delayed Start"])
const FINAL = new Set(["Final", "Game Over"])

function ordinal(n: number) {
  const s = ["th", "st", "nd", "rd"]
  const v = n % 100
  return n + (s[(v - 20) % 10] || s[v] || s[0])
}

function formatVolume(vol: string): string {
  const n = parseFloat(vol)
  if (isNaN(n) || n === 0) return "$0"
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1000) return `$${(n / 1000).toFixed(1)}K`
  return `$${n.toFixed(0)}`
}

function formatPrice(price: number): string {
  // Show cents with sub-cent precision when the price has it (Polymarket ticks
  // go down to 0.0001 = 0.01¢). Trim trailing zeros so 99.5¢ stays "99.5¢"
  // and 99¢ stays "99¢".
  const cents = price * 100
  const str = cents.toFixed(2).replace(/\.?0+$/, "")
  return `${str}¢`
}

function teamNick(fullName: string): string {
  // "Chicago Cubs" → "Cubs", "Tampa Bay Rays" → "Rays", "St. Louis Cardinals" → "Cardinals"
  const parts = fullName.split(" ")
  return parts[parts.length - 1]
}

function shortName(outcomeName: string, homeTeam: string, awayTeam: string): string {
  if (outcomeName.toLowerCase().includes(homeTeam.toLowerCase())) return teamNick(homeTeam)
  if (outcomeName.toLowerCase().includes(awayTeam.toLowerCase())) return teamNick(awayTeam)
  if (outcomeName.toLowerCase().startsWith("over")) return `O ${outcomeName.replace(/^over\s*/i, "")}`
  if (outcomeName.toLowerCase().startsWith("under")) return `U ${outcomeName.replace(/^under\s*/i, "")}`
  if (outcomeName === "Yes Run") return "Yes"
  if (outcomeName === "No Run") return "No"
  return outcomeName.length > 10 ? outcomeName.slice(0, 8) + "…" : outcomeName
}

// ── Scoreboard ───────────────────────────────────
function Scoreboard({ game }: { game: GameInfo }) {
  const isLive = LIVE.has(game.status)
  const isFinal = FINAL.has(game.status)
  const arrow = game.inning_state === "Top" ? "▲" : game.inning_state === "Bottom" ? "▼" : "◆"

  return (
    <Card>
      <CardContent className="p-6">
        <div className="flex items-center justify-center gap-8">
          <div className="text-center min-w-[120px]">
            <div className="text-sm text-foreground/70 uppercase tracking-widest font-semibold">{game.away_team_name}</div>
            <div className="text-5xl font-black tabular-nums mt-1">{game.away_score}</div>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Badge variant={isLive ? "live" : isFinal ? "final" : "secondary"}>
              {isLive ? "LIVE" : game.status}
            </Badge>
            {isLive && (
              <div className="text-lg font-semibold">
                {arrow} {ordinal(game.current_inning)}
              </div>
            )}
            {isFinal && game.current_inning > 9 && (
              <div className="text-sm text-muted-foreground">F/{game.current_inning}</div>
            )}
            <div className="text-xs text-muted-foreground">{game.venue_name}</div>
          </div>
          <div className="text-center min-w-[120px]">
            <div className="text-sm text-foreground/70 uppercase tracking-widest font-semibold">{game.home_team_name}</div>
            <div className="text-5xl font-black tabular-nums mt-1">{game.home_score}</div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

// ── Linescore ────────────────────────────────────
function Linescore({ innings, game }: { innings: LinescoreInning[]; game: GameInfo }) {
  const maxInn = Math.max(9, innings.length)
  return (
    <Card>
      <CardContent className="p-0 overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left px-3 py-2 text-xs text-muted-foreground w-[100px]"></th>
              {Array.from({ length: maxInn }, (_, i) => (
                <th key={i} className="px-2 py-2 text-xs text-muted-foreground font-semibold min-w-[32px]">{i + 1}</th>
              ))}
              <th className="px-3 py-2 text-xs text-muted-foreground font-bold bg-secondary/50">R</th>
            </tr>
          </thead>
          <tbody>
            <tr className="border-b border-border">
              <td className="px-3 py-2 font-bold text-xs uppercase tracking-wider">{game.away_team_name}</td>
              {Array.from({ length: maxInn }, (_, i) => {
                const inn = innings.find((x) => x.inning === i + 1)
                return <td key={i} className="px-2 py-2 text-center tabular-nums">{inn?.away ?? ""}</td>
              })}
              <td className="px-3 py-2 text-center font-bold bg-secondary/50 tabular-nums">{game.away_score}</td>
            </tr>
            <tr>
              <td className="px-3 py-2 font-bold text-xs uppercase tracking-wider">{game.home_team_name}</td>
              {Array.from({ length: maxInn }, (_, i) => {
                const inn = innings.find((x) => x.inning === i + 1)
                return <td key={i} className="px-2 py-2 text-center tabular-nums">{inn?.home ?? ""}</td>
              })}
              <td className="px-3 py-2 text-center font-bold bg-secondary/50 tabular-nums">{game.home_score}</td>
            </tr>
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

// ── Strike Zone ──────────────────────────────────
function StrikeZone({ pitches }: { pitches: Pitch[] }) {
  const [tooltip, setTooltip] = useState<{ pitch: Pitch; x: number; y: number } | null>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  let szTop = 3.5, szBottom = 1.6
  const valid = pitches.filter((p) => p.szTop && p.szBottom)
  if (valid.length > 0) {
    szTop = valid[valid.length - 1].szTop!
    szBottom = valid[valid.length - 1].szBottom!
  }

  const plateW = 17 / 12
  const half = plateW / 2

  function handleMouse(p: Pitch, e: React.MouseEvent) {
    const rect = svgRef.current?.getBoundingClientRect()
    if (!rect) return
    setTooltip({ pitch: p, x: e.clientX - rect.left + 12, y: e.clientY - rect.top - 10 })
  }

  return (
    <div className="relative flex flex-col items-center">
      <svg ref={svgRef} viewBox="-2 0 4 5" className="w-full max-w-[260px]">
        {/* Zone */}
        <rect x={-half} y={5 - szTop} width={plateW} height={szTop - szBottom}
          fill="none" stroke="white" strokeWidth={0.02} />
        {/* Grid */}
        {[1, 2].map((i) => (
          <g key={i}>
            <line x1={-half + (plateW / 3) * i} y1={5 - szTop} x2={-half + (plateW / 3) * i} y2={5 - szBottom}
              stroke="rgba(255,255,255,0.12)" strokeWidth={0.01} />
            <line x1={-half} y1={5 - szTop + ((szTop - szBottom) / 3) * i} x2={half} y2={5 - szTop + ((szTop - szBottom) / 3) * i}
              stroke="rgba(255,255,255,0.12)" strokeWidth={0.01} />
          </g>
        ))}
        {/* Plate */}
        <polygon
          points={`${-half},${5 - szBottom + 0.25} ${-half},${5 - szBottom + 0.45} 0,${5 - szBottom + 0.6} ${half},${5 - szBottom + 0.45} ${half},${5 - szBottom + 0.25}`}
          fill="none" stroke="rgba(255,255,255,0.25)" strokeWidth={0.015}
        />
        {/* Pitches */}
        {pitches.map((p) => {
          if (p.pX == null || p.pZ == null) return null
          const color = PITCH_COLORS[p.type_code] || "#888"
          return (
            <g key={p.num}>
              <circle cx={p.pX} cy={5 - p.pZ} r={0.08} fill={color}
                stroke="rgba(0,0,0,0.5)" strokeWidth={0.015}
                className="cursor-pointer hover:stroke-white hover:stroke-[0.03]"
                onMouseEnter={(e) => handleMouse(p, e)}
                onMouseLeave={() => setTooltip(null)}
              />
              <text x={p.pX} y={5 - p.pZ} textAnchor="middle" dominantBaseline="central"
                fill="white" fontSize={0.1} fontWeight={700} className="pointer-events-none">
                {p.num}
              </text>
            </g>
          )
        })}
      </svg>
      {tooltip && (
        <div className="absolute z-10 bg-black/90 text-white text-xs rounded-lg px-3 py-2 pointer-events-none whitespace-nowrap"
          style={{ left: tooltip.x, top: tooltip.y }}>
          <div className="font-bold">#{tooltip.pitch.num} {tooltip.pitch.type_desc || tooltip.pitch.type_code}</div>
          <div>{tooltip.pitch.speed?.toFixed(1)} mph · {tooltip.pitch.call}</div>
          {tooltip.pitch.spin_rate && <div>Spin: {tooltip.pitch.spin_rate} rpm</div>}
          {tooltip.pitch.hit_speed && (
            <div>Exit: {tooltip.pitch.hit_speed.toFixed(1)} mph, {tooltip.pitch.hit_angle?.toFixed(0)}°</div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Diamond ──────────────────────────────────────
function Diamond({ runners }: { runners: Runners }) {
  const baseClass = (occupied: boolean) =>
    `transition-colors duration-300 ${occupied ? "fill-yellow-400 stroke-yellow-400" : "fill-secondary stroke-white/30"}`

  return (
    <div className="flex flex-col items-center">
      <svg viewBox="0 0 100 100" className="w-full max-w-[160px]">
        <path d="M50,82 L78,50 L50,18 L22,50 Z" fill="none" stroke="rgba(255,255,255,0.1)" strokeWidth={1} />
        <rect x={44} y={12} width={12} height={12} rx={1} transform="rotate(45,50,18)"
          className={baseClass(!!runners.second)} strokeWidth={2} />
        <rect x={16} y={44} width={12} height={12} rx={1} transform="rotate(45,22,50)"
          className={baseClass(!!runners.third)} strokeWidth={2} />
        <rect x={72} y={44} width={12} height={12} rx={1} transform="rotate(45,78,50)"
          className={baseClass(!!runners.first)} strokeWidth={2} />
        <polygon points="50,79 54,84 50,89 46,84" fill="rgba(255,255,255,0.2)" />
      </svg>
      <div className="text-xs text-muted-foreground space-y-0.5 mt-1">
        {runners.first && <div>1B: {runners.first.name}</div>}
        {runners.second && <div>2B: {runners.second.name}</div>}
        {runners.third && <div>3B: {runners.third.name}</div>}
      </div>
    </div>
  )
}

// ── Count ────────────────────────────────────────
function CountDisplay({ count }: { count: Count }) {
  return (
    <div className="flex gap-5 justify-center">
      <div className="flex items-center gap-1.5">
        <span className="text-xs font-bold text-muted-foreground w-3">B</span>
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className={`w-3.5 h-3.5 rounded-full border-2 transition-colors duration-200 ${
            i < count.balls ? "bg-emerald-400 border-emerald-400" : "border-muted-foreground/40"
          }`} />
        ))}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-xs font-bold text-muted-foreground w-3">S</span>
        {[0, 1].map((i) => (
          <div key={i} className={`w-3.5 h-3.5 rounded-full border-2 transition-colors duration-200 ${
            i < count.strikes ? "bg-red-400 border-red-400" : "border-muted-foreground/40"
          }`} />
        ))}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-xs font-bold text-muted-foreground w-3">O</span>
        {[0, 1].map((i) => (
          <div key={i} className={`w-3.5 h-3.5 rounded-full border-2 transition-colors duration-200 ${
            i < count.outs ? "bg-yellow-400 border-yellow-400" : "border-muted-foreground/40"
          }`} />
        ))}
      </div>
    </div>
  )
}

// ── Matchup ──────────────────────────────────────
function Matchup({ ab }: { ab: GameState["current_ab"] }) {
  if (!ab) return <div className="text-muted-foreground text-sm">Waiting for at-bat...</div>

  const batLabel = ab.bat_side === "R" ? "Right" : ab.bat_side === "L" ? "Left" : "Switch"
  const pitchLabel = ab.pitch_hand === "R" ? "Right" : "Left"

  return (
    <div className="space-y-4">
      <div>
        <div className="text-xs text-muted-foreground uppercase tracking-wider">Batting</div>
        <div className="text-lg font-bold mt-0.5">{ab.batter_name}</div>
        <div className="text-xs text-muted-foreground">Bats: {batLabel}</div>
      </div>
      <div className="h-px bg-border" />
      <div>
        <div className="text-xs text-muted-foreground uppercase tracking-wider">Pitching</div>
        <div className="text-lg font-bold mt-0.5">{ab.pitcher_name}</div>
        <div className="text-xs text-muted-foreground">Throws: {pitchLabel}</div>
      </div>
      {ab.is_complete && ab.result && (
        <div className="bg-secondary rounded-lg p-3 text-sm">
          <span className="font-bold text-primary">{ab.result}</span>
          {ab.result_description && <span className="text-muted-foreground"> — {ab.result_description}</span>}
        </div>
      )}
    </div>
  )
}

// ── Pitch List ───────────────────────────────────
function PitchList({ pitches }: { pitches: Pitch[] }) {
  return (
    <div className="max-h-[320px] overflow-y-auto">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-card">
          <tr className="border-b border-border">
            <th className="text-left px-3 py-2 text-xs text-muted-foreground">#</th>
            <th className="text-left px-3 py-2 text-xs text-muted-foreground">Pitch</th>
            <th className="text-left px-3 py-2 text-xs text-muted-foreground">MPH</th>
            <th className="text-left px-3 py-2 text-xs text-muted-foreground">Result</th>
          </tr>
        </thead>
        <tbody>
          {pitches.map((p) => {
            const color = PITCH_COLORS[p.type_code] || "#888"
            return (
              <tr key={p.num} className="border-b border-border/50 hover:bg-secondary/30 transition-colors">
                <td className="px-3 py-2 tabular-nums">{p.num}</td>
                <td className="px-3 py-2">
                  <span className="inline-block w-2.5 h-2.5 rounded-full mr-2 align-middle" style={{ background: color }} />
                  {p.type_desc || p.type_code || "—"}
                </td>
                <td className="px-3 py-2 tabular-nums">{p.speed?.toFixed(1) ?? "—"}</td>
                <td className="px-3 py-2 text-muted-foreground">{p.call || "—"}</td>
              </tr>
            )
          })}
          {pitches.length === 0 && (
            <tr><td colSpan={4} className="px-3 py-6 text-center text-muted-foreground text-sm">No pitches yet</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

// ── Play-by-Play ─────────────────────────────────
function PlayByPlay({ plays }: { plays: Play[] }) {
  return (
    <div className="max-h-[320px] overflow-y-auto space-y-0">
      {plays.map((p) => (
        <div key={p.atBatIndex} className={`px-4 py-3 border-b border-border/50 ${p.is_scoring ? "border-l-2 border-l-yellow-400" : ""}`}>
          <div className="flex justify-between items-center mb-1">
            <span className="text-xs text-muted-foreground">
              {p.is_top ? "Top" : "Bot"} {ordinal(p.inning)}
            </span>
            <span className="text-xs font-bold tabular-nums">{p.away_score} - {p.home_score}</span>
          </div>
          <div className="text-sm">
            <span className="font-bold text-primary">{p.event}</span>
            <span className="text-muted-foreground"> — {p.batter}</span>
          </div>
          <div className="text-xs text-muted-foreground mt-0.5 leading-relaxed">{p.description}</div>
        </div>
      ))}
      {plays.length === 0 && (
        <div className="px-4 py-8 text-center text-muted-foreground text-sm">No plays yet</div>
      )}
    </div>
  )
}

// ── Order Book Display ──────────────────────────
type BookLevel = { price: number; size: number; total: number }

// Cumulative USDC depth from the spread outward (Polymarket's "Total" column).
// Backend already returns one entry per native tick, so no cent-aggregation.
function withCumulative(levels: OrderBookLevel[]): BookLevel[] {
  let cum = 0
  return levels.map(l => {
    cum += l.price * l.size
    return { price: l.price, size: l.size, total: cum }
  })
}

const MAX_LEVELS = 10

function OrderBookDisplay({ book }: { book: OrderBook | null }) {
  if (!book) return null

  // Asks: ascending by price (best/lowest first). Bids: descending (best/highest first).
  const asksBestFirst = [...book.asks].sort((a, b) => a.price - b.price).slice(0, MAX_LEVELS)
  const bidsBestFirst = [...book.bids].sort((a, b) => b.price - a.price).slice(0, MAX_LEVELS)

  const asksWithTotal = withCumulative(asksBestFirst)
  const bidsWithTotal = withCumulative(bidsBestFirst)

  // Display order: asks reversed so worst is at top, best sits right above the spread.
  // Bids stay best-first so best is right below the spread.
  const displayAsks = [...asksWithTotal].reverse()
  const displayBids = bidsWithTotal

  // Shared scale so ask/bid depth bars are directly comparable.
  const maxTotal = Math.max(
    asksWithTotal[asksWithTotal.length - 1]?.total ?? 0,
    bidsWithTotal[bidsWithTotal.length - 1]?.total ?? 0,
    1
  )

  const empty = displayAsks.length === 0 && displayBids.length === 0

  return (
    <div className="border border-border rounded-lg bg-card p-4 space-y-2">
      <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Order Book</div>

      {empty ? (
        <div className="text-xs text-muted-foreground text-center py-3">
          Liquidity provided by AMM — no limit orders on book
        </div>
      ) : (
        <div className="grid grid-cols-[1fr_100px_120px_120px] text-[11px] tabular-nums gap-y-px mt-1">
          {/* Header */}
          <div />
          <div className="text-center py-1 text-muted-foreground font-medium">PRICE</div>
          <div className="text-right py-1 text-muted-foreground font-medium pr-2">SHARES</div>
          <div className="text-right py-1 text-muted-foreground font-medium">TOTAL</div>
          {/* Asks (worst at top, best ask right above the spread) */}
          {displayAsks.map((level, i) => {
            const barWidth = (level.total / maxTotal) * 100
            return (
              <div key={`ask-${i}`} className="grid grid-cols-subgrid col-span-4 items-center">
                <div className="relative h-5">
                  <div className="absolute inset-y-0 right-0 bg-red-500/10 rounded-sm" style={{ width: `${barWidth}%` }} />
                </div>
                <div className="text-center text-red-400 font-medium">{formatPrice(level.price)}</div>
                <div className="text-right pr-2">{level.size.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                <div className="text-right">${level.total.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
              </div>
            )
          })}
          {/* Spread divider */}
          <div className="col-span-4 flex items-center justify-between py-1.5 my-1 border-y border-border/50 text-[10px] text-muted-foreground gap-x-3">
            <span>Last: <span className="text-foreground font-medium tabular-nums">{formatPrice(book.last_trade_price)}</span></span>
            <span>Spread: <span className="text-foreground font-medium tabular-nums">{formatPrice(Math.max(0, book.spread))}</span></span>
          </div>
          {/* Bids (best bid at top, right below the spread) */}
          {displayBids.map((level, i) => {
            const barWidth = (level.total / maxTotal) * 100
            return (
              <div key={`bid-${i}`} className="grid grid-cols-subgrid col-span-4 items-center">
                <div className="relative h-5">
                  <div className="absolute inset-y-0 right-0 bg-emerald-500/10 rounded-sm" style={{ width: `${barWidth}%` }} />
                </div>
                <div className="text-center text-emerald-400 font-medium">{formatPrice(level.price)}</div>
                <div className="text-right pr-2">{level.size.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                <div className="text-right">${level.total.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Order Slip ──────────────────────────────────
function OrderSlip({ market, outcomeIdx, home, away, gamePk, onSwitchOutcome }: {
  market: Market; outcomeIdx: number; home: string; away: string; gamePk: number; onSwitchOutcome: (idx: number) => void
}) {
  const [side, setSide] = useState<"buy" | "sell">("buy")
  const [amount, setAmount] = useState("")
  const outcome = market.outcomes[outcomeIdx]
  const book = market.order_books?.[outcomeIdx]

  // Derive price from order book
  const price = book ? (side === "buy" ? (book.ask ?? outcome.price) : (book.bid ?? outcome.price)) : outcome.price

  const amountNum = parseFloat(amount) || 0
  const shares = price > 0 ? amountNum / price : 0
  const payout = shares

  const matchTitle = `${teamNick(away)} vs ${teamNick(home)}`
  const selectedName = shortName(outcome.name, home, away)
  const lineStr = market.line != null ? ` ${market.line > 0 ? "+" : ""}${market.line}` : ""
  const selectedLabel = `${selectedName}${lineStr}`

  return (
    <div className="border border-border rounded-lg bg-card overflow-hidden">
      {/* Header */}
      <div className="px-4 pt-4 pb-2">
        <div className="text-xs text-muted-foreground">{matchTitle}</div>
        <div className="text-sm font-bold mt-0.5" style={{ color: getOutcomeAccentColor(market, outcomeIdx) }}>{selectedLabel}</div>
      </div>

      <div className="px-4 pb-4 space-y-4">
        {/* Buy / Sell tabs */}
        <div className="flex border-b border-border">
          <button
            onClick={() => setSide("buy")}
            className={`flex-1 pb-2 text-sm font-medium cursor-pointer transition-colors ${
              side === "buy" ? "text-white border-b-2 border-white" : "text-muted-foreground hover:text-foreground"
            }`}
          >Buy</button>
          <button
            onClick={() => setSide("sell")}
            className={`flex-1 pb-2 text-sm font-medium cursor-pointer transition-colors ${
              side === "sell" ? "text-white border-b-2 border-white" : "text-muted-foreground hover:text-foreground"
            }`}
          >Sell</button>
        </div>

        {/* Outcome toggle */}
        <div className="flex gap-1.5">
          {market.outcomes.map((o, i) => {
            const isActive = outcomeIdx === i
            const name = shortName(o.name, home, away)
            const lineLabel = market.line != null ? ` ${i === 0 ? (market.line > 0 ? "+" : "") : (market.line > 0 ? "-" : "+")}${Math.abs(market.line)}` : ""
            const colorCls = isActive
              ? getOutcomeColor(market, i, true)
              : "bg-[#1e1e2e] border-white/[0.06] text-muted-foreground"
            return (
              <button
                key={i}
                onClick={() => onSwitchOutcome(i)}
                className={`flex-1 px-2 py-2 rounded-lg text-xs font-medium cursor-pointer transition-all border ${colorCls}`}
              >
                {name}{lineLabel} <span className="font-bold ml-1 tabular-nums">{formatPrice(o.price)}</span>
              </button>
            )
          })}
        </div>

        {/* Amount */}
        <div className="space-y-2">
          <div className="text-xs text-muted-foreground">Amount</div>
          <div className="text-3xl font-bold text-center tabular-nums py-2">${amountNum}</div>
          <div className="flex gap-1.5">
            {[100, 500, 1000, 3000].map((v) => (
              <button
                key={v}
                onClick={() => setAmount(String((parseFloat(amount) || 0) + v))}
                className="flex-1 py-1.5 rounded-lg text-xs font-medium bg-[#1e1e2e] border border-white/[0.06] text-muted-foreground hover:text-foreground hover:bg-[#252538] cursor-pointer transition-colors tabular-nums"
              >+${v >= 1000 ? `${v/1000}K` : v}</button>
            ))}
            <button
              onClick={() => setAmount("")}
              className="flex-1 py-1.5 rounded-lg text-xs font-medium bg-[#1e1e2e] border border-white/[0.06] text-muted-foreground hover:text-foreground hover:bg-[#252538] cursor-pointer transition-colors"
            >Clear</button>
          </div>
        </div>

        {/* Summary */}
        {amountNum > 0 && (
          <div className="text-xs space-y-1 text-muted-foreground">
            <div className="flex justify-between">
              <span>Shares</span>
              <span className="tabular-nums text-foreground">{shares.toFixed(2)}</span>
            </div>
            <div className="flex justify-between">
              <span>Avg price</span>
              <span className="tabular-nums text-foreground">{formatPrice(price)}</span>
            </div>
            <div className="flex justify-between">
              <span>Potential return</span>
              <span className="tabular-nums font-bold text-emerald-400">${payout.toFixed(2)}{price > 0 ? ` (${((1 / price - 1) * 100).toFixed(0)}%)` : ""}</span>
            </div>
          </div>
        )}

        {/* Trade button */}
        <button
          disabled={amountNum <= 0}
          onClick={() => {
            if (amountNum <= 0) return
            try {
              const raw = localStorage.getItem("gameday_orders")
              const all: StoredOrder[] = raw ? JSON.parse(raw) : []
              const order: StoredOrder = {
                id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
                game_pk: gamePk,
                market_type: market.type,
                outcome_name: outcome.name,
                side,
                price,
                amount: amountNum,
                shares,
                timestamp: Date.now(),
              }
              all.push(order)
              localStorage.setItem("gameday_orders", JSON.stringify(all))
              window.dispatchEvent(new Event("gameday_orders_updated"))
              setAmount("")
            } catch {}
          }}
          className={`w-full py-3 rounded-lg text-sm font-bold text-white transition-colors ${
            amountNum > 0 ? "bg-[#3b82f6] hover:bg-[#3b82f6]/90 cursor-pointer" : "bg-[#3b82f6]/40 cursor-not-allowed"
          }`}
        >
          Place Order
        </button>

        <div className="text-[10px] text-center text-muted-foreground/60">
          Orders are saved locally (Phase 2 will wire to Polymarket). <a href="https://polymarket.com/tos" target="_blank" rel="noopener noreferrer" className="underline hover:text-foreground">Terms</a>.
        </div>
      </div>
    </div>
  )
}

function getOutcomeAccentColor(market: Market, idx: number): string {
  if (market.type === "spread") return "#e74c3c"
  if (market.type === "total") {
    const name = market.outcomes[idx]?.name?.toLowerCase() ?? ""
    return name.startsWith("over") ? "#2ecc71" : "#e74c3c"
  }
  return "#ffffff"
}

// ── Market Section ──────────────────────────────
function getOutcomeColor(market: Market, idx: number, isSelected: boolean): string {
  if (!isSelected) return "bg-[#1e1e2e] border-white/[0.08] text-foreground/80 hover:bg-[#252538]"
  if (market.type === "moneyline") return "bg-[#2d2d3d] border-white/20 text-white"
  if (market.type === "spread") return "bg-[#c0392b]/80 border-[#c0392b]/40 text-white"
  if (market.type === "total") {
    const name = market.outcomes[idx]?.name?.toLowerCase() ?? ""
    if (name.startsWith("over")) return "bg-[#27ae60]/80 border-[#27ae60]/40 text-white"
    return "bg-[#c0392b]/80 border-[#c0392b]/40 text-white"
  }
  if (market.type === "nrfi") {
    const name = market.outcomes[idx]?.name?.toLowerCase() ?? ""
    if (name.includes("no")) return "bg-[#27ae60]/80 border-[#27ae60]/40 text-white"
    return "bg-[#c0392b]/80 border-[#c0392b]/40 text-white"
  }
  return "bg-[#2d2d3d] border-white/20 text-white"
}

function MarketSection({ market, home, away, onSelect, selectedIdx }: {
  market: Market; home: string; away: string;
  onSelect: (idx: number) => void; selectedIdx: number | null
}) {
  const label = market.type === "nrfi" ? "NRFI" : market.type === "spread" ? "Spread" : market.type === "total" ? "Total" : "Moneyline"
  return (
    <div className="space-y-1.5">
      <div className="flex items-baseline gap-2">
        <span className="text-xs font-bold uppercase tracking-wider text-foreground/80">{label}</span>
        <span className="text-xs text-muted-foreground">{formatVolume(market.volume)} Vol.</span>
      </div>
      <div className="flex gap-1.5">
        {market.outcomes.map((o, i) => {
          const isSelected = selectedIdx === i
          const colorClasses = getOutcomeColor(market, i, isSelected)
          return (
            <button
              key={i}
              onClick={() => onSelect(i)}
              className={`flex-1 flex items-center justify-between px-3 py-2.5 rounded-lg text-xs transition-all cursor-pointer border ${colorClasses}`}
            >
              <span className="truncate">{shortName(o.name, home, away)}</span>
              <span className="tabular-nums font-bold ml-2">{formatPrice(o.price)}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}

// ── Betting Panel ───────────────────────────────
function BettingPanel({ odds, game }: { odds: OddsData; game: GameInfo }) {
  const [selected, setSelected] = useState<{ marketType: string; outcomeIdx: number }>({ marketType: "moneyline", outcomeIdx: 0 })
  const [spreadIdx, setSpreadIdx] = useState(0)
  const [totalIdx, setTotalIdx] = useState(0)

  if (!odds.available || odds.markets.length === 0) return null

  const byType: Record<string, Market[]> = {}
  for (const m of odds.markets) {
    (byType[m.type] ??= []).push(m)
  }

  const moneyline = byType["moneyline"]?.[0] ?? null
  const spreads = byType["spread"] ?? []
  const totals = byType["total"] ?? []
  const nrfi = byType["nrfi"]?.[0] ?? null
  const currentSpread = spreads[spreadIdx] ?? spreads[0]
  const currentTotal = totals[totalIdx] ?? totals[0]

  const home = game.home_team_name
  const away = game.away_team_name

  // Find the selected market object (defaults to moneyline first outcome)
  let selectedMarket: Market | null = null
  if (selected.marketType === "spread") selectedMarket = currentSpread
  else if (selected.marketType === "total") selectedMarket = currentTotal
  else if (selected.marketType === "nrfi") selectedMarket = nrfi
  else selectedMarket = moneyline
  const selectedOutcomeIdx = selected.outcomeIdx

  // Get order book for currently selected outcome
  const selectedBook: OrderBook | null = selectedMarket?.order_books?.[selectedOutcomeIdx] ?? null

  function handleSelect(marketType: string, idx: number) {
    setSelected({ marketType, outcomeIdx: idx })
  }

  function handleSwitchOutcome(idx: number) {
    setSelected({ marketType: selected.marketType, outcomeIdx: idx })
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Polymarket Odds</CardTitle>
          {odds.event_slug && (
            <a
              href={`https://polymarket.com/event/${odds.event_slug}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              View on Polymarket →
            </a>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col md:flex-row gap-4">
          {/* Market sections with inline order book */}
          <div className="flex-1 space-y-4">
            {moneyline && (
              <MarketSection
                market={moneyline} home={home} away={away}
                onSelect={(i) => handleSelect("moneyline", i)}
                selectedIdx={selected?.marketType === "moneyline" ? selected.outcomeIdx : null}
              />
            )}
            {selected?.marketType === "moneyline" && <OrderBookDisplay book={selectedBook} />}

            {currentSpread && (
              <div className="space-y-2">
                <MarketSection
                  market={currentSpread} home={home} away={away}
                  onSelect={(i) => handleSelect("spread", i)}
                  selectedIdx={selected?.marketType === "spread" ? selected.outcomeIdx : null}
                />
                {spreads.length > 1 && (
                  <div className="flex items-center justify-center gap-3 text-xs">
                    <button onClick={() => setSpreadIdx(Math.max(0, spreadIdx - 1))} className="text-muted-foreground hover:text-foreground cursor-pointer">&lt;</button>
                    <span className="tabular-nums font-bold">{currentSpread?.line ?? spreadIdx + 1}</span>
                    <button onClick={() => setSpreadIdx(Math.min(spreads.length - 1, spreadIdx + 1))} className="text-muted-foreground hover:text-foreground cursor-pointer">&gt;</button>
                  </div>
                )}
              </div>
            )}
            {selected?.marketType === "spread" && <OrderBookDisplay book={selectedBook} />}

            {currentTotal && (
              <div className="space-y-2">
                <MarketSection
                  market={currentTotal} home={home} away={away}
                  onSelect={(i) => handleSelect("total", i)}
                  selectedIdx={selected?.marketType === "total" ? selected.outcomeIdx : null}
                />
                {totals.length > 1 && (
                  <div className="flex items-center justify-center gap-3 text-xs">
                    <button onClick={() => setTotalIdx(Math.max(0, totalIdx - 1))} className="text-muted-foreground hover:text-foreground cursor-pointer">&lt;</button>
                    <span className="tabular-nums font-bold">{currentTotal?.line ?? totalIdx + 1}</span>
                    <button onClick={() => setTotalIdx(Math.min(totals.length - 1, totalIdx + 1))} className="text-muted-foreground hover:text-foreground cursor-pointer">&gt;</button>
                  </div>
                )}
              </div>
            )}
            {selected?.marketType === "total" && <OrderBookDisplay book={selectedBook} />}

            {nrfi && (moneyline || currentSpread || currentTotal) && (
              <div className="pt-3 border-t border-border">
                <MarketSection
                  market={nrfi} home={home} away={away}
                  onSelect={(i) => handleSelect("nrfi", i)}
                  selectedIdx={selected?.marketType === "nrfi" ? selected.outcomeIdx : null}
                />
              </div>
            )}
            {nrfi && !moneyline && !currentSpread && !currentTotal && (
              <MarketSection
                market={nrfi} home={home} away={away}
                onSelect={(i) => handleSelect("nrfi", i)}
                selectedIdx={selected?.marketType === "nrfi" ? selected.outcomeIdx : null}
              />
            )}
            {selected?.marketType === "nrfi" && <OrderBookDisplay book={selectedBook} />}
          </div>

          {/* Sticky order slip on right */}
          {selectedMarket && (
            <div className="w-full md:w-72 shrink-0 md:sticky md:top-4 md:self-start space-y-4">
              <OrderSlip
                market={selectedMarket}
                outcomeIdx={selectedOutcomeIdx}
                home={home}
                away={away}
                gamePk={game.game_pk}
                onSwitchOutcome={handleSwitchOutcome}
              />
              <YourOrders gamePk={game.game_pk} home={home} away={away} />
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

// ── Your Orders ─────────────────────────────────
interface StoredOrder {
  id: string
  game_pk: number
  market_type: string
  outcome_name: string
  side: "buy" | "sell"
  price: number
  amount: number
  shares: number
  timestamp: number
}

function loadOrders(gamePk: number): StoredOrder[] {
  try {
    const raw = localStorage.getItem("gameday_orders")
    if (!raw) return []
    const all: StoredOrder[] = JSON.parse(raw)
    return all.filter(o => o.game_pk === gamePk).sort((a, b) => b.timestamp - a.timestamp)
  } catch { return [] }
}

function YourOrders({ gamePk, home, away }: { gamePk: number; home: string; away: string }) {
  const [orders, setOrders] = useState<StoredOrder[]>(() => loadOrders(gamePk))

  useEffect(() => {
    setOrders(loadOrders(gamePk))
    const onStorage = () => setOrders(loadOrders(gamePk))
    window.addEventListener("storage", onStorage)
    window.addEventListener("gameday_orders_updated", onStorage)
    return () => {
      window.removeEventListener("storage", onStorage)
      window.removeEventListener("gameday_orders_updated", onStorage)
    }
  }, [gamePk])

  function removeOrder(id: string) {
    try {
      const raw = localStorage.getItem("gameday_orders")
      if (!raw) return
      const all: StoredOrder[] = JSON.parse(raw)
      const filtered = all.filter(o => o.id !== id)
      localStorage.setItem("gameday_orders", JSON.stringify(filtered))
      window.dispatchEvent(new Event("gameday_orders_updated"))
    } catch {}
  }

  return (
    <div className="border border-border rounded-lg p-4 bg-card">
      <div className="text-xs font-bold uppercase tracking-wider text-foreground/80 mb-2">Your Orders</div>
      {orders.length === 0 ? (
        <div className="text-xs text-muted-foreground py-2">No orders yet</div>
      ) : (
        <div className="space-y-1.5">
          {orders.map((o) => {
            const label = shortName(o.outcome_name, home, away)
            const sideColor = o.side === "buy" ? "text-emerald-400" : "text-red-400"
            return (
              <div key={o.id} className="flex items-center justify-between text-xs py-1.5 px-2 rounded bg-white/[0.03] border border-white/[0.05]">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-1.5">
                    <span className="text-muted-foreground capitalize">{o.market_type}</span>
                    <span className="font-medium truncate">{label}</span>
                    <span className="tabular-nums font-bold">{formatPrice(o.price)}</span>
                    <span className={`${sideColor} font-semibold uppercase`}>{o.side}</span>
                  </div>
                  <div className="text-muted-foreground text-[10px] tabular-nums mt-0.5">
                    ${o.amount.toFixed(2)} → {o.shares.toFixed(2)} shares
                  </div>
                </div>
                <button
                  onClick={() => removeOrder(o.id)}
                  className="text-muted-foreground/60 hover:text-red-400 text-xs ml-2 cursor-pointer"
                  title="Remove"
                >
                  ✕
                </button>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Home Page ──────────────────────────────────────
function HomePage() {
  const [games, setGames] = useState<GameInfo[]>([])
  const [date, setDate] = useState(() => {
    const now = new Date()
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`
  })


  useEffect(() => {
    fetch(`/api/games?date=${date}`).then(r => r.json()).then(setGames).catch(() => {})
  }, [date])

  const changeDate = (delta: number) => {
    const d = new Date(date + "T12:00:00")
    d.setDate(d.getDate() + delta)
    setDate(d.toISOString().slice(0, 10))
  }

  // Format date for display
  const displayDate = new Date(date + "T12:00:00").toLocaleDateString("en-US", {
    weekday: "long", month: "long", day: "numeric", year: "numeric"
  })

  // Sort: live games first, then by game_datetime
  const sorted = [...games].sort((a, b) => {
    const al = LIVE.has(a.status) ? 0 : 1
    const bl = LIVE.has(b.status) ? 0 : 1
    if (al !== bl) return al - bl
    return (a.game_datetime || "").localeCompare(b.game_datetime || "")
  })

  return (
    <div className="min-h-screen bg-background">
      <div className="max-w-2xl mx-auto p-4">
        {/* Header */}
        <h1 className="text-2xl font-bold mb-6">MLB Gameday</h1>

        {/* Date navigation */}
        <div className="flex items-center justify-between mb-6">
          <button onClick={() => changeDate(-1)}
            className="px-3 py-1.5 rounded-md bg-secondary text-sm hover:bg-secondary/80 transition-colors cursor-pointer">
            &larr; Prev
          </button>
          <div className="text-center">
            <div className="text-lg font-semibold">{displayDate}</div>
            <button onClick={() => setDate(new Date().toISOString().slice(0, 10))}
              className="text-xs text-muted-foreground hover:text-foreground cursor-pointer mt-1">
              Today
            </button>
          </div>
          <button onClick={() => changeDate(1)}
            className="px-3 py-1.5 rounded-md bg-secondary text-sm hover:bg-secondary/80 transition-colors cursor-pointer">
            Next &rarr;
          </button>
        </div>

        {/* Game list */}
        <div className="space-y-1">
          {sorted.map(g => {
            const isLive = LIVE.has(g.status)
            const isFinal = FINAL.has(g.status)
            const arrow = g.inning_state === "Top" ? "\u25B2" : g.inning_state === "Bottom" ? "\u25BC" : ""

            return (
              <Link
                key={g.game_pk}
                to={g.slug ? `/game/${g.slug}` : `/game/${g.game_pk}`}
                className="flex items-center justify-between px-4 py-3 rounded-lg hover:bg-secondary/50 transition-colors group"
              >
                <div className="flex items-center gap-4 min-w-0">
                  {isLive && <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse shrink-0" />}
                  {!isLive && <span className="w-2 h-2 shrink-0" />}
                  <div className="flex items-center gap-3 text-sm min-w-0">
                    <div className="w-32 flex justify-between">
                      <span className="font-medium text-foreground">{teamNick(g.away_team_name)}</span>
                      <span className="font-bold tabular-nums text-foreground">{g.away_score ?? 0}</span>
                    </div>
                    <span className="text-muted-foreground/40">@</span>
                    <div className="w-32 flex justify-between">
                      <span className="font-medium text-foreground">{teamNick(g.home_team_name)}</span>
                      <span className="font-bold tabular-nums text-foreground">{g.home_score ?? 0}</span>
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  {isLive && (
                    <span className="text-xs font-semibold text-emerald-400">
                      {arrow} {ordinal(g.current_inning)}
                    </span>
                  )}
                  {isFinal && (
                    <span className="text-xs text-muted-foreground">
                      {g.current_inning > 9 ? `F/${g.current_inning}` : "Final"}
                    </span>
                  )}
                  {!isLive && !isFinal && (
                    <span className="text-xs text-muted-foreground">{g.status}</span>
                  )}
                  <span className="text-muted-foreground/30 group-hover:text-muted-foreground transition-colors">&rarr;</span>
                </div>
              </Link>
            )
          })}
          {games.length === 0 && (
            <div className="text-center py-12 text-muted-foreground">No games scheduled</div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Game Page ──────────────────────────────────────
function GamePage() {
  const { slug } = useParams()

  const [gamePk, setGamePk] = useState<number | null>(null)
  const [state, setState] = useState<GameState | null>(null)
  const [odds, setOdds] = useState<OddsData | null>(null)
  const esRef = useRef<EventSource | null>(null)

  // Resolve slug to game_pk
  useEffect(() => {
    if (!slug) return
    // Try numeric game_pk first
    if (/^\d+$/.test(slug)) {
      setGamePk(parseInt(slug))
      return
    }
    fetch(`/api/game/by-slug/${slug}`)
      .then(r => r.json())
      .then(d => { if (d.game_pk) setGamePk(d.game_pk) })
      .catch(() => {})
  }, [slug])

  // SSE stream
  useEffect(() => {
    if (!gamePk) return
    const controller = new AbortController()
    fetch(`/api/game/${gamePk}`, { signal: controller.signal })
      .then(r => r.json()).then(setState).catch(() => {})
    const es = new EventSource(`/api/game/${gamePk}/stream`)
    es.onmessage = (e) => setState(JSON.parse(e.data))
    esRef.current = es
    return () => { controller.abort(); es.close(); esRef.current = null }
  }, [gamePk])

  // Odds stream: initial REST snapshot for fast paint, then SSE for live updates
  // pushed from the backend's Polymarket WS bridge.
  useEffect(() => {
    if (!gamePk) { setOdds(null); return }
    const controller = new AbortController()
    fetch(`/api/game/${gamePk}/odds`, { signal: controller.signal })
      .then(r => r.json()).then(setOdds).catch(() => {})
    const es = new EventSource(`/api/game/${gamePk}/odds/stream`)
    es.onmessage = (e) => setOdds(JSON.parse(e.data))
    return () => { controller.abort(); es.close() }
  }, [gamePk])

  if (!state) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-6xl mx-auto p-4">
          <Link to="/" className="text-sm text-muted-foreground hover:text-foreground mb-4 inline-block">&larr; All Games</Link>
          <div className="text-center py-20 text-muted-foreground text-lg">Loading game...</div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="max-w-6xl mx-auto p-4 space-y-3">
        <Link to="/" className="text-sm text-muted-foreground hover:text-foreground inline-block">&larr; All Games</Link>
        <Scoreboard game={state.game} />
        {odds && odds.available && <BettingPanel odds={odds} game={state.game} />}
        <Linescore innings={state.linescore} game={state.game} />
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <Card>
            <CardHeader><CardTitle>Strike Zone</CardTitle></CardHeader>
            <CardContent><StrikeZone pitches={state.pitches} /></CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle>Bases</CardTitle></CardHeader>
            <CardContent>
              <Diamond runners={state.runners} />
              <div className="mt-4"><CountDisplay count={state.count} /></div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle>Matchup</CardTitle></CardHeader>
            <CardContent><Matchup ab={state.current_ab} /></CardContent>
          </Card>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <Card>
            <CardHeader><CardTitle>Pitch Sequence</CardTitle></CardHeader>
            <CardContent className="p-0"><PitchList pitches={state.pitches} /></CardContent>
          </Card>
          <Card className="md:col-span-2">
            <CardHeader><CardTitle>Play-by-Play</CardTitle></CardHeader>
            <CardContent className="p-0"><PlayByPlay plays={state.plays} /></CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

// ── Portfolio Page ─────────────────────────────────

function formatUsd(n: number): string {
  const abs = Math.abs(n)
  const sign = n < 0 ? "-" : ""
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(2)}M`
  if (abs >= 10_000) return `${sign}$${abs.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
  return `${sign}$${abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function formatPct(n: number): string {
  const sign = n >= 0 ? "+" : ""
  return `${sign}${n.toFixed(2)}%`
}

function formatRelativeTime(unixSeconds: number): string {
  const diff = Date.now() / 1000 - unixSeconds
  if (diff < 60) return `${Math.floor(diff)}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function PortfolioSummary({ p }: { p: Portfolio }) {
  const pnlColor = p.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"
  return (
    <Card>
      <CardContent className="p-6">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
          <div>
            <div className="text-xs text-muted-foreground uppercase tracking-wider">Portfolio Value</div>
            <div className="text-3xl font-black tabular-nums mt-1">{formatUsd(p.total_value)}</div>
            <div className="text-xs text-muted-foreground mt-1 tabular-nums">
              {p.position_count} position{p.position_count === 1 ? "" : "s"}
            </div>
          </div>
          <div>
            <div className="text-xs text-muted-foreground uppercase tracking-wider">Unrealized P/L</div>
            <div className={`text-3xl font-black tabular-nums mt-1 ${pnlColor}`}>
              {formatUsd(p.total_pnl)}
            </div>
            <div className={`text-xs mt-1 tabular-nums ${pnlColor}`}>
              {formatPct(p.percent_pnl)}
            </div>
          </div>
          <div>
            <div className="text-xs text-muted-foreground uppercase tracking-wider">Cost Basis</div>
            <div className="text-3xl font-black tabular-nums mt-1">{formatUsd(p.total_cost)}</div>
            <div className="text-xs text-muted-foreground mt-1">Entry amount</div>
          </div>
          <div>
            <div className="text-xs text-muted-foreground uppercase tracking-wider">Realized P/L</div>
            <div className={`text-3xl font-black tabular-nums mt-1 ${p.total_realized >= 0 ? "text-emerald-400" : "text-red-400"}`}>
              {formatUsd(p.total_realized)}
            </div>
            <div className="text-xs text-muted-foreground mt-1">Locked in</div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function PositionsTable({ positions }: { positions: Position[] }) {
  if (positions.length === 0) {
    return <div className="text-center py-12 text-muted-foreground text-sm">No open positions</div>
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-card z-10">
          <tr className="border-b border-border">
            <th className="text-left px-3 py-2 text-xs text-muted-foreground font-semibold">Market</th>
            <th className="text-left px-3 py-2 text-xs text-muted-foreground font-semibold">Side</th>
            <th className="text-right px-3 py-2 text-xs text-muted-foreground font-semibold">Shares</th>
            <th className="text-right px-3 py-2 text-xs text-muted-foreground font-semibold">Avg</th>
            <th className="text-right px-3 py-2 text-xs text-muted-foreground font-semibold">Current</th>
            <th className="text-right px-3 py-2 text-xs text-muted-foreground font-semibold">Value</th>
            <th className="text-right px-3 py-2 text-xs text-muted-foreground font-semibold">P/L</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((pos) => {
            const pnlColor = pos.cash_pnl >= 0 ? "text-emerald-400" : "text-red-400"
            const href = pos.event_slug ? `https://polymarket.com/event/${pos.event_slug}` : undefined
            return (
              <tr key={pos.asset ?? pos.condition_id ?? pos.title} className="border-b border-border/40 hover:bg-secondary/30 transition-colors">
                <td className="px-3 py-2.5 max-w-[340px]">
                  <div className="flex items-center gap-2 min-w-0">
                    {pos.icon && <img src={pos.icon} alt="" className="w-6 h-6 rounded object-cover shrink-0" />}
                    {href ? (
                      <a href={href} target="_blank" rel="noreferrer" className="min-w-0 truncate block font-medium hover:text-primary transition-colors">
                        {pos.title}
                      </a>
                    ) : (
                      <span className="min-w-0 truncate font-medium">{pos.title}</span>
                    )}
                  </div>
                </td>
                <td className="px-3 py-2.5">
                  <span className="inline-flex items-center gap-1 text-xs">
                    <span className="px-1.5 py-0.5 rounded bg-secondary text-foreground/80 font-semibold">
                      {pos.outcome}
                    </span>
                    {pos.is_live && <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" title="Live price" />}
                  </span>
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums">{pos.size.toFixed(2)}</td>
                <td className="px-3 py-2.5 text-right tabular-nums text-muted-foreground">{formatPrice(pos.avg_price)}</td>
                <td className="px-3 py-2.5 text-right tabular-nums">{formatPrice(pos.cur_price)}</td>
                <td className="px-3 py-2.5 text-right tabular-nums font-semibold">{formatUsd(pos.current_value)}</td>
                <td className={`px-3 py-2.5 text-right tabular-nums font-semibold ${pnlColor}`}>
                  <div>{formatUsd(pos.cash_pnl)}</div>
                  <div className="text-[10px] font-normal">{formatPct(pos.percent_pnl)}</div>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function ActivityFeed({ items }: { items: Activity[] }) {
  if (items.length === 0) {
    return <div className="text-center py-8 text-muted-foreground text-sm">No recent activity</div>
  }
  return (
    <div className="max-h-[600px] overflow-y-auto">
      {items.map((a, i) => {
        const sideColor = a.side === "BUY" ? "text-emerald-400" : "text-red-400"
        const href = a.eventSlug ? `https://polymarket.com/event/${a.eventSlug}` : undefined
        return (
          <div key={`${a.transactionHash}-${a.conditionId ?? a.asset ?? i}`} className="px-4 py-3 border-b border-border/50 hover:bg-secondary/20 transition-colors">
            <div className="flex items-center justify-between gap-3 mb-1">
              <div className="flex items-center gap-2 min-w-0">
                {a.icon && <img src={a.icon} alt="" className="w-5 h-5 rounded object-cover shrink-0" />}
                {href ? (
                  <a href={href} target="_blank" rel="noreferrer" className="text-sm font-medium truncate hover:text-primary transition-colors">
                    {a.title}
                  </a>
                ) : (
                  <span className="text-sm font-medium truncate">{a.title}</span>
                )}
              </div>
              <span className="text-xs text-muted-foreground shrink-0 tabular-nums">{formatRelativeTime(a.timestamp)}</span>
            </div>
            <div className="flex items-center gap-2 text-xs">
              <span className={`font-bold uppercase ${sideColor}`}>{a.side}</span>
              <span className="px-1.5 py-0.5 rounded bg-secondary text-foreground/70">{a.outcome}</span>
              <span className="text-muted-foreground tabular-nums">
                {a.size.toFixed(2)} @ {formatPrice(a.price)}
              </span>
              <span className="text-muted-foreground tabular-nums ml-auto">{formatUsd(a.usdcSize)}</span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function PortfolioPage() {
  const { username } = useParams()
  const user = username ?? "whycantilose"
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null)
  const [activity, setActivity] = useState<Activity[]>([])

  useEffect(() => {
    const controller = new AbortController()
    fetch(`/api/portfolio/${user}`, { signal: controller.signal })
      .then(r => r.json()).then(setPortfolio).catch(() => {})
    const es = new EventSource(`/api/portfolio/${user}/stream`)
    es.onmessage = (e) => setPortfolio(JSON.parse(e.data))
    return () => { controller.abort(); es.close() }
  }, [user])

  useEffect(() => {
    let cancelled = false
    const load = () => {
      fetch(`/api/portfolio/${user}/activity`)
        .then(r => r.json())
        .then(d => { if (!cancelled) setActivity(d.activity ?? []) })
        .catch(() => {})
    }
    load()
    const id = setInterval(load, 15000)
    return () => { cancelled = true; clearInterval(id) }
  }, [user])

  if (!portfolio) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-6xl mx-auto p-4">
          <Link to="/" className="text-sm text-muted-foreground hover:text-foreground mb-4 inline-block">&larr; All Games</Link>
          <div className="text-center py-20 text-muted-foreground text-lg">Loading portfolio...</div>
        </div>
      </div>
    )
  }

  if (!portfolio.available) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-6xl mx-auto p-4">
          <Link to="/" className="text-sm text-muted-foreground hover:text-foreground mb-4 inline-block">&larr; All Games</Link>
          <div className="text-center py-20 text-muted-foreground text-lg">
            {portfolio.loading ? "Loading..." : `No portfolio for @${user}`}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="max-w-6xl mx-auto p-4 space-y-3">
        <div className="flex items-center justify-between">
          <Link to="/" className="text-sm text-muted-foreground hover:text-foreground">&larr; All Games</Link>
          <div className="text-right">
            <div className="text-sm font-semibold">@{portfolio.username ?? user}</div>
            {portfolio.wallet && (
              <div className="text-[10px] text-muted-foreground font-mono">
                {portfolio.wallet.slice(0, 6)}…{portfolio.wallet.slice(-4)}
              </div>
            )}
          </div>
        </div>

        <PortfolioSummary p={portfolio} />

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
          <Card className="lg:col-span-2">
            <CardHeader><CardTitle>Positions</CardTitle></CardHeader>
            <CardContent className="p-0"><PositionsTable positions={portfolio.positions} /></CardContent>
          </Card>
          <Card>
            <CardHeader><CardTitle>Activity</CardTitle></CardHeader>
            <CardContent className="p-0"><ActivityFeed items={activity} /></CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

// ── App (Router) ──────────────────────────────────
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/game/:slug" element={<GamePage />} />
      <Route path="/portfolio/:username" element={<PortfolioPage />} />
      <Route path="/portfolio" element={<PortfolioPage />} />
    </Routes>
  )
}
