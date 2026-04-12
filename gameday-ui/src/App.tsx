import { useEffect, useRef, useState } from "react"
import { Routes, Route, Link, useParams } from "react-router-dom"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import type { GameInfo, GameState, Pitch, Runners, Count, Play, LinescoreInning, OddsData, Market, OrderBook, OrderBookLevel, Portfolio, Position, PnlPoint, PnlInterval } from "./types"

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
            <div className="text-sm text-[#5e5d59] uppercase tracking-widest font-semibold">{game.away_team_name}</div>
            <div className="text-5xl font-black tabular-nums mt-1 text-[#141413]">{game.away_score}</div>
          </div>
          <div className="flex flex-col items-center gap-2">
            <Badge variant={isLive ? "live" : isFinal ? "final" : "secondary"}>
              {isLive ? "LIVE" : game.status}
            </Badge>
            {isLive && (
              <div className="text-lg font-semibold text-[#c96442]">
                {arrow} {ordinal(game.current_inning)}
              </div>
            )}
            {isFinal && game.current_inning > 9 && (
              <div className="text-sm text-muted-foreground">F/{game.current_inning}</div>
            )}
            <div className="text-xs text-muted-foreground">{game.venue_name}</div>
          </div>
          <div className="text-center min-w-[120px]">
            <div className="text-sm text-[#5e5d59] uppercase tracking-widest font-semibold">{game.home_team_name}</div>
            <div className="text-5xl font-black tabular-nums mt-1 text-[#141413]">{game.home_score}</div>
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
          fill="none" stroke="#4d4c48" strokeWidth={0.02} />
        {/* Grid */}
        {[1, 2].map((i) => (
          <g key={i}>
            <line x1={-half + (plateW / 3) * i} y1={5 - szTop} x2={-half + (plateW / 3) * i} y2={5 - szBottom}
              stroke="rgba(77,76,72,0.15)" strokeWidth={0.01} />
            <line x1={-half} y1={5 - szTop + ((szTop - szBottom) / 3) * i} x2={half} y2={5 - szTop + ((szTop - szBottom) / 3) * i}
              stroke="rgba(77,76,72,0.15)" strokeWidth={0.01} />
          </g>
        ))}
        {/* Plate */}
        <polygon
          points={`${-half},${5 - szBottom + 0.25} ${-half},${5 - szBottom + 0.45} 0,${5 - szBottom + 0.6} ${half},${5 - szBottom + 0.45} ${half},${5 - szBottom + 0.25}`}
          fill="none" stroke="rgba(77,76,72,0.3)" strokeWidth={0.015}
        />
        {/* Pitches */}
        {pitches.map((p) => {
          if (p.pX == null || p.pZ == null) return null
          const color = PITCH_COLORS[p.type_code] || "#888"
          return (
            <g key={p.num}>
              <circle cx={p.pX} cy={5 - p.pZ} r={0.08} fill={color}
                stroke="rgba(0,0,0,0.5)" strokeWidth={0.015}
                className="cursor-pointer hover:stroke-[#141413] hover:stroke-[0.03]"
                onMouseEnter={(e) => handleMouse(p, e)}
                onMouseLeave={() => setTooltip(null)}
              />
              <text x={p.pX} y={5 - p.pZ} textAnchor="middle" dominantBaseline="central"
                fill="#141413" fontSize={0.1} fontWeight={700} className="pointer-events-none">
                {p.num}
              </text>
            </g>
          )
        })}
      </svg>
      {tooltip && (
        <div className="absolute z-10 bg-[#141413] text-[#faf9f5] text-xs rounded-lg px-3 py-2 pointer-events-none whitespace-nowrap shadow-[0px_0px_0px_1px_#30302e]"
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
    `transition-colors duration-300 ${occupied ? "fill-[#c96442] stroke-[#c96442]" : "fill-secondary stroke-[#87867f]/40"}`

  return (
    <div className="flex flex-col items-center">
      <svg viewBox="0 0 100 100" className="w-full max-w-[160px]">
        <path d="M50,82 L78,50 L50,18 L22,50 Z" fill="none" stroke="rgba(77,76,72,0.15)" strokeWidth={1} />
        <rect x={44} y={12} width={12} height={12} rx={1} transform="rotate(45,50,18)"
          className={baseClass(!!runners.second)} strokeWidth={2} />
        <rect x={16} y={44} width={12} height={12} rx={1} transform="rotate(45,22,50)"
          className={baseClass(!!runners.third)} strokeWidth={2} />
        <rect x={72} y={44} width={12} height={12} rx={1} transform="rotate(45,78,50)"
          className={baseClass(!!runners.first)} strokeWidth={2} />
        <polygon points="50,79 54,84 50,89 46,84" fill="rgba(77,76,72,0.25)" />
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
            i < count.balls ? "bg-emerald-600 border-emerald-600" : "border-muted-foreground/40"
          }`} />
        ))}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-xs font-bold text-muted-foreground w-3">S</span>
        {[0, 1].map((i) => (
          <div key={i} className={`w-3.5 h-3.5 rounded-full border-2 transition-colors duration-200 ${
            i < count.strikes ? "bg-[#b53333] border-red-300" : "border-muted-foreground/40"
          }`} />
        ))}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-xs font-bold text-muted-foreground w-3">O</span>
        {[0, 1].map((i) => (
          <div key={i} className={`w-3.5 h-3.5 rounded-full border-2 transition-colors duration-200 ${
            i < count.outs ? "bg-[#c96442] border-[#c96442]" : "border-muted-foreground/40"
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
        <div key={p.atBatIndex} className={`px-4 py-3 border-b border-border/50 ${p.is_scoring ? "border-l-2 border-l-[#c96442]" : ""}`}>
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
type BookLevel = { price: number; size: number; notional: number; cumulative: number }

// Enrich raw levels with per-level $ notional (drives bar width) and cumulative
// $ depth from the spread outward (Polymarket's "Total" column). Backend returns
// one entry per native tick, so no cent-aggregation here.
function enrichLevels(levels: OrderBookLevel[]): BookLevel[] {
  let cum = 0
  return levels.map(l => {
    const notional = l.price * l.size
    cum += notional
    return { price: l.price, size: l.size, notional, cumulative: cum }
  })
}

const MAX_LEVELS = 7
const DUST_NOTIONAL = 3 // USDC — levels below this render dimmed (dust vs real liquidity)

function OrderBookDisplay({ book }: { book: OrderBook | null }) {
  if (!book) return null

  // Asks: ascending by price (best/lowest first). Bids: descending (best/highest first).
  const asksBestFirst = [...book.asks].sort((a, b) => a.price - b.price).slice(0, MAX_LEVELS)
  const bidsBestFirst = [...book.bids].sort((a, b) => b.price - a.price).slice(0, MAX_LEVELS)

  const asks = enrichLevels(asksBestFirst)
  const bids = enrichLevels(bidsBestFirst)

  // Per-level notional (not cumulative) drives bar width. Cumulative visually
  // flattens the top-of-book differences — 96/97/98/99¢ cumulative values end
  // up ~75-100% of the same max, so bars look nearly identical. Per-level
  // reveals the true size distribution at each tick.
  const maxNotional = Math.max(
    ...asks.map(l => l.notional),
    ...bids.map(l => l.notional),
    1
  )

  // Pad to a fixed row count so the container height never changes as levels
  // come and go. Outer-edge padding keeps best-bid/best-ask anchored to the
  // spread divider — the user's visual reference point never moves.
  const askPadCount = Math.max(0, MAX_LEVELS - asks.length)
  const bidPadCount = Math.max(0, MAX_LEVELS - bids.length)
  const askRows: (BookLevel | null)[] = [
    ...Array(askPadCount).fill(null),
    ...[...asks].reverse(), // worst at top, best ask right above the spread
  ]
  const bidRows: (BookLevel | null)[] = [
    ...bids, // best at top, right below the spread
    ...Array(bidPadCount).fill(null),
  ]

  const empty = asks.length === 0 && bids.length === 0

  const renderRow = (
    level: BookLevel | null,
    isBest: boolean,
    side: "ask" | "bid",
    key: string,
  ) => {
    if (!level) {
      // Rest marker for empty slots — keeps the grid rhythm without feeling void.
      return (
        <div key={key} className="grid grid-cols-subgrid col-span-4 items-center h-5">
          <div />
          <div className="text-center text-muted-foreground/25 font-mono leading-none">·</div>
          <div />
          <div />
        </div>
      )
    }
    const barWidth = (level.notional / maxNotional) * 100
    const isDust = level.notional < DUST_NOTIONAL
    const rowOpacity = isDust && !isBest ? "opacity-40" : "opacity-100"
    const barClass = side === "ask" ? "bg-[#b53333]/10" : "bg-emerald-700/10"
    const textClass = side === "ask" ? "text-[#b53333]" : "text-emerald-700"
    const markerClass = side === "ask" ? "bg-[#b53333]" : "bg-emerald-700"
    return (
      <div
        key={key}
        className={`grid grid-cols-subgrid col-span-4 items-center h-5 transition-opacity duration-200 ${rowOpacity}`}
      >
        <div className="relative h-full">
          <div
            className={`absolute inset-y-[3px] right-0 ${barClass} rounded-[2px] transition-[width] duration-200 ease-out`}
            style={{ width: `${barWidth}%` }}
          />
          {isBest && (
            <div className={`absolute right-0 inset-y-[2px] w-[2px] rounded-full ${markerClass}`} />
          )}
        </div>
        <div className={`text-center font-mono ${textClass} ${isBest ? "font-semibold" : "font-medium"}`}>
          {formatPrice(level.price)}
        </div>
        <div className="text-right pr-2 font-mono text-foreground/85">
          {level.size.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </div>
        <div className="text-right font-mono text-foreground/55">
          ${level.cumulative.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </div>
      </div>
    )
  }

  return (
    <div className="border border-border rounded-lg bg-card p-4 space-y-2">
      <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Order Book</div>

      {empty ? (
        <div className="text-xs text-muted-foreground text-center py-3">
          Liquidity provided by AMM — no limit orders on book
        </div>
      ) : (
        <div className="grid grid-cols-[1fr_88px_108px_120px] text-[11px] gap-y-px mt-1">
          {/* Header */}
          <div />
          <div className="text-center pb-1.5 text-muted-foreground/70 font-medium text-[9px] tracking-[0.15em] uppercase">Price</div>
          <div className="text-right pb-1.5 pr-2 text-muted-foreground/70 font-medium text-[9px] tracking-[0.15em] uppercase">Shares</div>
          <div className="text-right pb-1.5 text-muted-foreground/70 font-medium text-[9px] tracking-[0.15em] uppercase">Total</div>

          {/* Asks (worst at top, best ask right above the spread) */}
          {askRows.map((level, i) =>
            renderRow(level, i === askRows.length - 1 && level !== null, "ask", `ask-${i}`)
          )}

          {/* Spread divider — "Spread" value sits under the PRICE column */}
          <div className="grid grid-cols-subgrid col-span-4 items-center py-1.5 my-0.5 border-y border-border/60 text-[10px] text-muted-foreground">
            <div className="font-mono">
              <span className="uppercase tracking-[0.1em] text-muted-foreground/70">Last</span>{" "}
              <span className="text-foreground/90 font-medium">{formatPrice(book.last_trade_price)}</span>
            </div>
            <div className="text-center font-mono">
              <span className="uppercase tracking-[0.1em] text-muted-foreground/70">Spread</span>{" "}
              <span className="text-foreground/90 font-semibold">
                {formatPrice(Math.max(0, book.spread))}
              </span>
            </div>
            <div />
            <div />
          </div>

          {/* Bids (best bid at top, right below the spread) */}
          {bidRows.map((level, i) =>
            renderRow(level, i === 0 && level !== null, "bid", `bid-${i}`)
          )}
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
              side === "buy" ? "text-[#141413] border-b-2 border-[#c96442]" : "text-muted-foreground hover:text-foreground"
            }`}
          >Buy</button>
          <button
            onClick={() => setSide("sell")}
            className={`flex-1 pb-2 text-sm font-medium cursor-pointer transition-colors ${
              side === "sell" ? "text-[#141413] border-b-2 border-[#c96442]" : "text-muted-foreground hover:text-foreground"
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
              : "bg-[#f0eee6] border-[#e8e6dc] text-muted-foreground"
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
                className="flex-1 py-1.5 rounded-lg text-xs font-medium bg-[#f0eee6] border border-[#e8e6dc] text-muted-foreground hover:text-foreground hover:bg-[#e8e6dc] cursor-pointer transition-colors tabular-nums"
              >+${v >= 1000 ? `${v/1000}K` : v}</button>
            ))}
            <button
              onClick={() => setAmount("")}
              className="flex-1 py-1.5 rounded-lg text-xs font-medium bg-[#f0eee6] border border-[#e8e6dc] text-muted-foreground hover:text-foreground hover:bg-[#e8e6dc] cursor-pointer transition-colors"
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
              <span className="tabular-nums font-bold text-emerald-700">${payout.toFixed(2)}{price > 0 ? ` (${((1 / price - 1) * 100).toFixed(0)}%)` : ""}</span>
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
          className={`w-full py-3 rounded-lg text-sm font-bold text-[#faf9f5] transition-colors ${
            amountNum > 0 ? "bg-[#c96442] hover:bg-[#d97757] cursor-pointer" : "bg-[#c96442]/40 cursor-not-allowed"
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
  if (!isSelected) return "bg-[#f0eee6] border-[#e8e6dc] text-foreground/80 hover:bg-[#e8e6dc]"
  if (market.type === "moneyline") return "bg-[#e8e6dc] border-[#d1cfc5] text-[#141413]"
  if (market.type === "spread") return "bg-[#b53333]/10 border-[#b53333]/25 text-[#b53333]"
  if (market.type === "total") {
    const name = market.outcomes[idx]?.name?.toLowerCase() ?? ""
    if (name.startsWith("over")) return "bg-emerald-700/10 border-emerald-700/25 text-emerald-800"
    return "bg-[#b53333]/10 border-[#b53333]/25 text-[#b53333]"
  }
  if (market.type === "nrfi") {
    const name = market.outcomes[idx]?.name?.toLowerCase() ?? ""
    if (name.includes("no")) return "bg-emerald-700/10 border-emerald-700/25 text-emerald-800"
    return "bg-[#b53333]/10 border-[#b53333]/25 text-[#b53333]"
  }
  return "bg-[#e8e6dc] border-[#d1cfc5] text-[#141413]"
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
            const sideColor = o.side === "buy" ? "text-emerald-700" : "text-[#b53333]"
            return (
              <div key={o.id} className="flex items-center justify-between text-xs py-1.5 px-2 rounded bg-[#f0eee6] border border-[#e8e6dc]">
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
                  className="text-muted-foreground/60 hover:text-[#b53333] text-xs ml-2 cursor-pointer"
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

// ── Mini Portfolio Bar (home page header strip) ─────
function MiniPortfolioBar({ user }: { user: string }) {
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    fetch(`/api/portfolio/${user}`, { signal: controller.signal })
      .then(r => r.json()).then(setPortfolio).catch(() => {})
    const es = new EventSource(`/api/portfolio/${user}/stream`)
    es.onmessage = (e) => setPortfolio(JSON.parse(e.data))
    return () => { controller.abort(); es.close() }
  }, [user])

  if (!portfolio?.available) return null

  const openGain = portfolio.total_pnl >= 0
  const lifetime = portfolio.lifetime_pnl ?? 0
  const lifetimeGain = lifetime >= 0
  const openColor = openGain ? "text-emerald-700" : "text-[#b53333]"
  const lifetimeColor = lifetimeGain ? "text-emerald-700" : "text-[#b53333]"
  const openSign = openGain ? "+" : "−"
  const lifetimeSign = lifetimeGain ? "+" : "−"

  const Stat = ({ label, value, color, align = "left" }: {
    label: string; value: string; color?: string; align?: "left" | "right"
  }) => (
    <div className={align === "right" ? "text-right" : ""}>
      <div className="text-[9px] uppercase tracking-[0.18em] text-muted-foreground/80">{label}</div>
      <div className={`text-lg font-bold tabular-nums leading-tight mt-0.5 ${color ?? "text-foreground"}`}>
        {value}
      </div>
    </div>
  )

  return (
    <Link
      to={`/portfolio/${user}`}
      className="group block rounded-xl border border-border/60 bg-card/40 backdrop-blur-sm hover:bg-card hover:border-border transition-colors px-4 py-3 mb-4"
    >
      <div className="flex items-center justify-between gap-4">
        <Stat label="Portfolio" value={formatUsd(portfolio.total_value)} />
        <div className="h-8 w-px bg-border/50" />
        <Stat
          label="Open P/L"
          value={`${openSign}${formatUsdCompact(Math.abs(portfolio.total_pnl))}`}
          color={openColor}
        />
        <div className="h-8 w-px bg-border/50" />
        <Stat
          label="All-Time"
          value={`${lifetimeSign}${formatUsdCompact(Math.abs(lifetime))}`}
          color={lifetimeColor}
          align="right"
        />
      </div>
    </Link>
  )
}

// All HomePage dates are **user-local calendar dates** — never UTC-derived.
// Using toISOString would shift the string by up to a day for users outside
// UTC, producing "Today" buttons that disagree with the user's own calendar.
function toLocalDateString(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`
}

// ── Home Page ──────────────────────────────────────
function HomePage() {
  const [games, setGames] = useState<GameInfo[]>([])
  const [date, setDate] = useState(() => toLocalDateString(new Date()))
  const todayLocal = toLocalDateString(new Date())

  useEffect(() => {
    fetch(`/api/games?date=${date}`).then(r => r.json()).then(setGames).catch(() => {})
  }, [date])

  const changeDate = (delta: number) => {
    const d = new Date(date + "T12:00:00")
    d.setDate(d.getDate() + delta)
    setDate(toLocalDateString(d))
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
        {/* Live portfolio strip */}
        <MiniPortfolioBar user="whycantilose" />

        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <h1 className="text-2xl font-serif" style={{ fontWeight: 500 }}>MLB Gameday</h1>
        </div>

        {/* Date navigation */}
        <div className="flex items-center justify-between mb-6">
          <button onClick={() => changeDate(-1)}
            className="px-3 py-1.5 rounded-md bg-secondary text-sm hover:bg-secondary/80 transition-colors cursor-pointer">
            &larr; Prev
          </button>
          <div className="text-center">
            <div className="text-lg font-serif" style={{ fontWeight: 500 }}>{displayDate}</div>
            {date !== todayLocal && (
              <button onClick={() => setDate(todayLocal)}
                className="mt-1.5 px-3 py-0.5 text-xs text-[#5e5d59] rounded-md border border-[#e8e6dc] hover:bg-[#e8e6dc] hover:text-[#141413] cursor-pointer transition-colors">
                Today
              </button>
            )}
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
                  {isLive && <span className="w-2 h-2 rounded-full bg-[#c96442] animate-pulse shrink-0" />}
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
                    <span className="text-xs font-semibold text-[#c96442]">
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

function formatUsdCompact(n: number): string {
  const abs = Math.abs(n)
  const sign = n < 0 ? "-" : ""
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(2)}M`
  if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`
  return `${sign}$${abs.toFixed(0)}`
}

function formatPct(n: number): string {
  const sign = n >= 0 ? "+" : ""
  return `${sign}${n.toFixed(2)}%`
}

type MarketType = "moneyline" | "spread" | "total" | "nrfi" | "other"

function inferMarketType(title: string, outcome: string): MarketType {
  const t = title.toLowerCase()
  const o = outcome.toLowerCase()
  if (t.startsWith("spread")) return "spread"
  if (t.includes("o/u") || o.startsWith("over") || o.startsWith("under")) return "total"
  if (t.includes("run in the first") || o.includes("yes run") || o.includes("no run")) return "nrfi"
  if (t.includes("vs")) return "moneyline"
  return "other"
}

const MARKET_TYPE_META: Record<MarketType, { label: string; dot: string; text: string }> = {
  moneyline: { label: "ML",   dot: "bg-[#c96442]",    text: "text-[#c96442]" },
  spread:    { label: "SPR",  dot: "bg-orange-600", text: "text-orange-700" },
  total:     { label: "O/U",  dot: "bg-violet-600", text: "text-violet-700" },
  nrfi:      { label: "NRFI", dot: "bg-amber-600",  text: "text-amber-700" },
  other:     { label: "···",  dot: "bg-muted-foreground", text: "text-muted-foreground" },
}

function MarketTypeBadge({ type }: { type: MarketType }) {
  const meta = MARKET_TYPE_META[type]
  return (
    <span className={`inline-flex items-center gap-1 text-[9px] font-bold uppercase tracking-[0.1em] ${meta.text}`}>
      <span className={`w-1 h-1 rounded-full ${meta.dot}`} />
      {meta.label}
    </span>
  )
}

// Derive a clean group header from a position title. Strip "Spread: " prefix
// and trailing " O/U X.5 Runs" to recover the bare matchup where possible.
function cleanMatchupTitle(title: string): string {
  return title
    .replace(/^Spread:\s*/i, "")
    .replace(/\s+O\/U\s+\d+\.?\d*\s*(Runs?)?$/i, "")
    .replace(/\s+\([+-]?\d+\.?\d*\)\s*$/, "")
}

interface EventGroup {
  key: string
  title: string
  icon: string | null
  eventSlug: string | null
  positions: Position[]
  totalValue: number
  totalCost: number
  totalPnl: number
  percentPnl: number
}

// Canonical pnl formulas. Mirrors _pnl_stats in gameday/app.py — the backend
// computes every top-level number; this helper only exists for UI-side
// aggregations like event grouping where we re-roll positions into buckets.
function pnlStats(currentValue: number, cost: number): { cashPnl: number; percentPnl: number } {
  const cashPnl = currentValue - cost
  return { cashPnl, percentPnl: cost > 0 ? (cashPnl / cost) * 100 : 0 }
}

function groupPositionsByEvent(positions: Position[]): EventGroup[] {
  const groups = new Map<string, Position[]>()
  for (const p of positions) {
    const key = p.event_slug ?? p.condition_id ?? p.title
    const arr = groups.get(key)
    if (arr) arr.push(p)
    else groups.set(key, [p])
  }
  const out: EventGroup[] = []
  for (const [key, poses] of groups) {
    const moneyline = poses.find(p => inferMarketType(p.title, p.outcome) === "moneyline")
    const representative = moneyline ?? poses[0]
    const totalValue = poses.reduce((s, p) => s + p.current_value, 0)
    const totalCost = poses.reduce((s, p) => s + p.initial_value, 0)
    const { cashPnl, percentPnl } = pnlStats(totalValue, totalCost)
    out.push({
      key,
      title: cleanMatchupTitle(representative.title),
      icon: representative.icon,
      eventSlug: representative.event_slug,
      positions: [...poses].sort((a, b) => b.current_value - a.current_value),
      totalValue,
      totalCost,
      totalPnl: cashPnl,
      percentPnl,
    })
  }
  return out.sort((a, b) => b.totalValue - a.totalValue)
}

// ── P/L Sparkline + timeframe scrubbing ─────────────────────────────
// Interactive SVG line chart with crosshair scrubbing and anchored tooltip.
// Emits hover changes upward so the hero's big number can respond.

const INTERVAL_LABELS: Record<PnlInterval, string> = {
  "1d": "1D",
  "1w": "1W",
  "1m": "1M",
  "all": "ALL",
}

// Duration in seconds → "2h 15m", "3d 4h", "45m", etc. Range selection
// shows this between the two anchors so the user can see elapsed time.
function formatDuration(totalSeconds: number): string {
  const s = Math.max(0, Math.round(totalSeconds))
  if (s < 60) return `${s}s`
  const mins = Math.floor(s / 60)
  if (mins < 60) return `${mins}m`
  const hrs = Math.floor(mins / 60)
  const remMins = mins % 60
  if (hrs < 48) return remMins ? `${hrs}h ${remMins}m` : `${hrs}h`
  const days = Math.floor(hrs / 24)
  const remHrs = hrs % 24
  return remHrs ? `${days}d ${remHrs}h` : `${days}d`
}

function fmtRange(t: number): string {
  return new Date(t * 1000).toLocaleString("en-US", {
    month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
  })
}

function TimeframeSelector({
  value, onChange, gain,
}: {
  value: PnlInterval
  onChange: (v: PnlInterval) => void
  gain: boolean
}) {
  const accent = gain ? "bg-emerald-600" : "bg-[#b53333]"
  return (
    <div className="inline-flex border border-border/60 bg-background/40 divide-x divide-border/60 select-none">
      {(["1d", "1w", "1m", "all"] as PnlInterval[]).map((tf) => {
        const active = tf === value
        return (
          <button
            key={tf}
            onClick={() => onChange(tf)}
            className={`relative px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.15em] tabular-nums transition-colors cursor-pointer ${
              active
                ? "text-foreground bg-card"
                : "text-muted-foreground hover:text-foreground hover:bg-card/50"
            }`}
          >
            {active && <span className={`absolute left-0 right-0 top-0 h-px ${accent}`} />}
            {INTERVAL_LABELS[tf]}
          </button>
        )
      })}
    </div>
  )
}

interface PnlRange {
  start: number
  end: number
}

interface SparklineProps {
  series: PnlPoint[]
  gain: boolean
  hoverIndex: number | null
  onHoverChange: (idx: number | null) => void
  range: PnlRange | null
  onRangeChange: (range: PnlRange | null) => void
  height?: number
}

function PnlSparkline({
  series, gain, hoverIndex, onHoverChange, range, onRangeChange, height = 140,
}: SparklineProps) {
  const svgRef = useRef<SVGSVGElement>(null)
  // Tracks the drag anchor (mousedown index) while the pointer is held.
  // null when not actively dragging.
  const dragAnchorRef = useRef<number | null>(null)
  const dragMovedRef = useRef<boolean>(false)
  const width = 640   // viewBox coordinate space; SVG scales via CSS

  if (series.length < 2) {
    return (
      <div
        className="rounded-lg border border-dashed border-border/40 text-[10px] text-muted-foreground flex items-center justify-center"
        style={{ height }}
      >
        Waiting for P/L history…
      </div>
    )
  }

  const xs = series.map(p => p.t)
  const ys = series.map(p => p.p)
  const minT = xs[0]
  const maxT = xs[xs.length - 1]
  const minP = Math.min(...ys, 0)
  const maxP = Math.max(...ys, 0)
  const padX = 6
  const padY = 14
  const w = width - padX * 2
  const h = height - padY * 2

  const tSpan = Math.max(1, maxT - minT)
  const pSpan = Math.max(1, maxP - minP)

  const xAt = (t: number) => padX + ((t - minT) / tSpan) * w
  const yAt = (p: number) => padY + h - ((p - minP) / pSpan) * h

  const pathD = series
    .map((pt, i) => `${i === 0 ? "M" : "L"}${xAt(pt.t).toFixed(1)},${yAt(pt.p).toFixed(1)}`)
    .join(" ")
  const areaD =
    `${pathD} ` +
    `L${xAt(maxT).toFixed(1)},${yAt(0).toFixed(1)} ` +
    `L${xAt(minT).toFixed(1)},${yAt(0).toFixed(1)} Z`

  const zeroY = yAt(0)
  const lineColor = gain ? "#34d399" : "#f87171"
  const fillColor = gain ? "rgba(52,211,153,0.20)" : "rgba(248,113,113,0.20)"
  const gradId = `pnlgrad-${gain ? "g" : "l"}`
  const lastX = xAt(xs[xs.length - 1])
  const lastY = yAt(ys[ys.length - 1])

  // Convert pointer X to the nearest series index.
  function pointerIndex(clientX: number): number | null {
    const rect = svgRef.current?.getBoundingClientRect()
    if (!rect) return null
    const localX = ((clientX - rect.left) / rect.width) * width
    const t = minT + ((localX - padX) / w) * tSpan
    let lo = 0, hi = series.length - 1
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      if (series[mid].t < t) lo = mid + 1
      else hi = mid
    }
    let idx = lo
    if (idx > 0 && Math.abs(series[idx - 1].t - t) < Math.abs(series[idx].t - t)) idx = idx - 1
    return idx
  }

  function handlePointerDown(clientX: number) {
    const idx = pointerIndex(clientX)
    if (idx == null) return
    dragAnchorRef.current = idx
    dragMovedRef.current = false
    onHoverChange(null)
    onRangeChange({ start: idx, end: idx })
  }

  function handlePointerMove(clientX: number) {
    const idx = pointerIndex(clientX)
    if (idx == null) return
    if (dragAnchorRef.current != null) {
      if (idx !== dragAnchorRef.current) dragMovedRef.current = true
      onRangeChange({ start: dragAnchorRef.current, end: idx })
    } else {
      onHoverChange(idx)
    }
  }

  function handlePointerUp() {
    // Keep the range locked after drag ends; treat a bare click (no
    // movement) as a "clear" gesture so the user can dismiss without
    // leaving the chart.
    if (dragAnchorRef.current == null) return
    const moved = dragMovedRef.current
    dragAnchorRef.current = null
    dragMovedRef.current = false
    if (!moved) onRangeChange(null)
  }

  function handlePointerLeave() {
    // Don't finalize mid-drag on leave — wait for an explicit up.
    if (dragAnchorRef.current == null) onHoverChange(null)
  }

  const hover = hoverIndex != null ? series[hoverIndex] : null
  const hoverX = hover ? xAt(hover.t) : 0
  const hoverY = hover ? yAt(hover.p) : 0

  // Normalize the range so a <= b regardless of drag direction.
  const rNorm = range
    ? { a: Math.min(range.start, range.end), b: Math.max(range.start, range.end) }
    : null
  const rStart = rNorm ? series[rNorm.a] : null
  const rEnd = rNorm ? series[rNorm.b] : null
  const rStartX = rStart ? xAt(rStart.t) : 0
  const rStartY = rStart ? yAt(rStart.p) : 0
  const rEndX = rEnd ? xAt(rEnd.t) : 0
  const rEndY = rEnd ? yAt(rEnd.p) : 0
  const rangeActive = rStart != null && rEnd != null
  const rangeDelta = rangeActive ? rEnd!.p - rStart!.p : 0
  const rangeUp = rangeDelta >= 0
  const rangeColor = rangeUp ? "#059669" : "#b53333"

  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${width} ${height}`}
      className="w-full h-full block cursor-crosshair touch-none select-none"
      preserveAspectRatio="none"
      onMouseDown={(e) => { e.preventDefault(); handlePointerDown(e.clientX) }}
      onMouseMove={(e) => handlePointerMove(e.clientX)}
      onMouseUp={handlePointerUp}
      onMouseLeave={handlePointerLeave}
      onTouchStart={(e) => { e.preventDefault(); if (e.touches[0]) handlePointerDown(e.touches[0].clientX) }}
      onTouchMove={(e) => { e.preventDefault(); if (e.touches[0]) handlePointerMove(e.touches[0].clientX) }}
      onTouchEnd={() => { handlePointerUp(); onHoverChange(null) }}
    >
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={fillColor} stopOpacity="1" />
          <stop offset="100%" stopColor={fillColor} stopOpacity="0" />
        </linearGradient>
      </defs>

      {/* Chart frame hairlines (top + bottom ticks at each end) */}
      <line x1={padX} x2={padX} y1={padY - 6} y2={padY - 1} stroke="rgba(20,20,19,0.15)" strokeWidth="0.6" />
      <line x1={width - padX} x2={width - padX} y1={padY - 6} y2={padY - 1} stroke="rgba(20,20,19,0.15)" strokeWidth="0.6" />
      <line x1={padX} x2={padX} y1={padY + h + 1} y2={padY + h + 6} stroke="rgba(20,20,19,0.15)" strokeWidth="0.6" />
      <line x1={width - padX} x2={width - padX} y1={padY + h + 1} y2={padY + h + 6} stroke="rgba(20,20,19,0.15)" strokeWidth="0.6" />

      {/* Zero baseline */}
      {minP < 0 && maxP > 0 && (
        <>
          <line
            x1={padX} x2={width - padX} y1={zeroY} y2={zeroY}
            stroke="rgba(20,20,19,0.10)" strokeWidth="0.5" strokeDasharray="2,3"
          />
          <text
            x={width - padX - 2} y={zeroY - 3}
            fontSize="8" fill="rgba(20,20,19,0.35)"
            textAnchor="end" fontFamily="ui-monospace, SFMono-Regular, monospace"
          >
            0
          </text>
        </>
      )}

      {/* Area fill + main line */}
      <path d={areaD} fill={`url(#${gradId})`} />
      <path
        d={pathD} fill="none" stroke={lineColor} strokeWidth="1.75"
        strokeLinejoin="round" strokeLinecap="round"
      />

      {/* Resting marker at current (rightmost) point */}
      {hover == null && !rangeActive && (
        <>
          <circle cx={lastX} cy={lastY} r="5" fill={lineColor} opacity="0.18">
            <animate attributeName="r" values="4;7;4" dur="2.4s" repeatCount="indefinite" />
            <animate attributeName="opacity" values="0.18;0.05;0.18" dur="2.4s" repeatCount="indefinite" />
          </circle>
          <circle cx={lastX} cy={lastY} r="2.2" fill={lineColor} />
        </>
      )}

      {/* Hover crosshair + square marker (surveyor's mark) */}
      {hover && !rangeActive && (
        <>
          {/* Vertical line (full height) */}
          <line
            x1={hoverX} x2={hoverX} y1={padY - 8} y2={padY + h + 8}
            stroke="rgba(20,20,19,0.25)" strokeWidth="0.6" strokeDasharray="1.5,2.5"
          />
          {/* Horizontal line (full width) */}
          <line
            x1={padX - 4} x2={width - padX + 4} y1={hoverY} y2={hoverY}
            stroke="rgba(20,20,19,0.15)" strokeWidth="0.6" strokeDasharray="1.5,2.5"
          />
          {/* Square marker rotated 45° */}
          <rect
            x={hoverX - 4} y={hoverY - 4} width="8" height="8"
            transform={`rotate(45 ${hoverX} ${hoverY})`}
            fill="none" stroke={lineColor} strokeWidth="1.4"
          />
          {/* Inner dot */}
          <circle cx={hoverX} cy={hoverY} r="1.6" fill={lineColor} />
        </>
      )}

      {/* Range selection overlay: shaded band + anchored markers + chord line */}
      {rangeActive && (
        <>
          {/* Shaded band between anchor and endpoint */}
          {rEndX !== rStartX && (
            <rect
              x={Math.min(rStartX, rEndX)}
              y={padY - 4}
              width={Math.abs(rEndX - rStartX)}
              height={h + 8}
              fill={rangeColor}
              fillOpacity="0.08"
            />
          )}
          {/* Vertical guides at both ends */}
          <line
            x1={rStartX} x2={rStartX} y1={padY - 8} y2={padY + h + 8}
            stroke={rangeColor} strokeOpacity="0.55" strokeWidth="0.8" strokeDasharray="2,2"
          />
          <line
            x1={rEndX} x2={rEndX} y1={padY - 8} y2={padY + h + 8}
            stroke={rangeColor} strokeOpacity="0.55" strokeWidth="0.8" strokeDasharray="2,2"
          />
          {/* Chord connecting base to current point — makes the slope legible */}
          {rEndX !== rStartX && (
            <line
              x1={rStartX} y1={rStartY} x2={rEndX} y2={rEndY}
              stroke={rangeColor} strokeOpacity="0.6" strokeWidth="1"
              strokeDasharray="3,2"
            />
          )}
          {/* Base anchor (hollow circle) */}
          <circle cx={rStartX} cy={rStartY} r="4.5" fill="#faf9f5" stroke={rangeColor} strokeWidth="1.6" />
          <circle cx={rStartX} cy={rStartY} r="1.4" fill={rangeColor} />
          {/* End marker (filled diamond) */}
          <rect
            x={rEndX - 4} y={rEndY - 4} width="8" height="8"
            transform={`rotate(45 ${rEndX} ${rEndY})`}
            fill={rangeColor} stroke={rangeColor} strokeWidth="1.4"
          />
        </>
      )}
    </svg>
  )
}

function PortfolioHero({ p, user }: { p: Portfolio; user: string }) {
  const openGain = p.total_pnl >= 0
  const openColor = openGain ? "text-emerald-700" : "text-[#b53333]"

  const lifetime = p.lifetime_pnl ?? 0
  const pnlMap = (p.pnl_series ?? {}) as Partial<Record<PnlInterval, PnlPoint[]>>

  const [timeframe, setTimeframe] = useState<PnlInterval>("all")
  const [hoverIndex, setHoverIndex] = useState<number | null>(null)
  const [range, setRange] = useState<PnlRange | null>(null)

  // Reset hover + range when switching timeframe — indices don't carry across
  useEffect(() => { setHoverIndex(null); setRange(null) }, [timeframe])

  const series = pnlMap[timeframe] ?? pnlMap.all ?? []

  // The number shown when not hovering:
  //  • all      → cumulative lifetime P/L (same as the last point)
  //  • 1d/1w/1m → delta between first and last point of the window
  const windowDelta = series.length >= 2
    ? series[series.length - 1].p - series[0].p
    : 0
  const defaultValue = timeframe === "all" ? lifetime : windowDelta
  const defaultLabel = timeframe === "all"
    ? "All-Time P/L"
    : `${INTERVAL_LABELS[timeframe]} Change`

  // Range selection takes priority over hover — normalize so start <= end
  const rangePts = range && series.length >= 2
    ? (() => {
        const a = Math.min(range.start, range.end)
        const b = Math.max(range.start, range.end)
        return { start: series[a], end: series[b], sameIdx: a === b }
      })()
    : null

  const hoverPoint = hoverIndex != null ? series[hoverIndex] : null

  let shownValue: number
  let shownLabel: string
  let rangePct: number | null = null
  let rangeDuration = ""

  if (rangePts) {
    const delta = rangePts.end.p - rangePts.start.p
    shownValue = delta
    const base = Math.abs(rangePts.start.p)
    rangePct = base > 0.5 ? (delta / base) * 100 : null
    rangeDuration = formatDuration(rangePts.end.t - rangePts.start.t)
    if (rangePts.sameIdx) {
      // Single-point anchor: show the base P/L as the hero number
      shownValue = rangePts.start.p
      shownLabel = `Base · ${fmtRange(rangePts.start.t)}`
    } else {
      shownLabel = `${fmtRange(rangePts.start.t)} → ${fmtRange(rangePts.end.t)}`
    }
  } else if (hoverPoint) {
    shownValue = hoverPoint.p
    shownLabel = new Date(hoverPoint.t * 1000).toLocaleString("en-US", {
      month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
    })
  } else {
    shownValue = defaultValue
    shownLabel = defaultLabel
  }

  const shownGain = shownValue >= 0
  const shownColor = shownGain ? "text-emerald-700" : "text-[#b53333]"

  // The ambient glow follows the dominant number currently on screen
  const glowColor = shownGain ? "rgba(4,120,87,0.08)" : "rgba(181,51,51,0.08)"

  const firstAllT = (pnlMap.all ?? [])[0]?.t
  const sinceLabel = firstAllT
    ? new Date(firstAllT * 1000).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
    : null

  // For the chart window label (start — end) under the chart
  const windowStart = series[0]?.t
  const windowEnd = series[series.length - 1]?.t
  const fmtShort = (t: number) =>
    new Date(t * 1000).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric" })

  return (
    <section className="relative overflow-hidden rounded-2xl border border-border/60 bg-card">
      {/* Dotted grid texture */}
      <div
        className="absolute inset-0 pointer-events-none opacity-[0.07]"
        style={{
          backgroundImage: "radial-gradient(circle, #87867f 1px, transparent 1px)",
          backgroundSize: "22px 22px",
        }}
      />
      {/* P/L-colored glow wash */}
      <div
        className="absolute right-0 top-0 w-[60%] h-full pointer-events-none blur-3xl transition-colors duration-500"
        style={{ background: `radial-gradient(ellipse at right, ${glowColor}, transparent 70%)` }}
      />
      {/* Hairline corner ticks */}
      <div className="absolute top-3 left-3 w-3 h-3 border-t border-l border-foreground/30" />
      <div className="absolute top-3 right-3 w-3 h-3 border-t border-r border-foreground/30" />
      <div className="absolute bottom-3 left-3 w-3 h-3 border-b border-l border-foreground/30" />
      <div className="absolute bottom-3 right-3 w-3 h-3 border-b border-r border-foreground/30" />

      <div className="relative px-6 md:px-10 pt-8 pb-8">
        {/* Top rail */}
        <div className="flex items-center justify-between gap-4 mb-8">
          <div className="flex items-center gap-3">
            <span className="text-[10px] uppercase tracking-[0.3em] text-muted-foreground">Portfolio</span>
            <span className="text-foreground/30">/</span>
            <span className="text-[11px] font-semibold text-foreground">@{p.username ?? user}</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="relative flex items-center gap-1.5 text-[10px] uppercase tracking-[0.2em] text-emerald-700">
              <span className="relative flex w-1.5 h-1.5">
                <span className="absolute inset-0 rounded-full bg-emerald-600 animate-ping opacity-60" />
                <span className="relative rounded-full w-1.5 h-1.5 bg-emerald-600" />
              </span>
              Live
            </span>
            {p.wallet && (
              <span className="text-[10px] text-muted-foreground font-mono tabular-nums">
                {p.wallet.slice(0, 6)}…{p.wallet.slice(-4)}
              </span>
            )}
          </div>
        </div>

        {/* Primary stats row: Portfolio Value · Open P/L */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 md:gap-10 mb-8">
          <div>
            <div className="text-[10px] uppercase tracking-[0.25em] text-muted-foreground mb-1">
              Portfolio Value
            </div>
            <div className="text-4xl md:text-5xl font-black tabular-nums leading-none tracking-tight text-foreground">
              {formatUsd(p.total_value)}
            </div>
            <div className="text-[11px] text-muted-foreground mt-2 tabular-nums">
              {p.position_count} position{p.position_count === 1 ? "" : "s"}
              <span className="mx-2 text-foreground/20">·</span>
              Cost {formatUsd(p.total_cost)}
            </div>
          </div>

          <div className="md:text-right">
            <div className="text-[10px] uppercase tracking-[0.25em] text-muted-foreground mb-1">
              Open Unrealized P/L
            </div>
            <div className={`text-4xl md:text-5xl font-black tabular-nums leading-none tracking-tight ${openColor}`}>
              {openGain ? "+" : "−"}${Math.abs(p.total_pnl).toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}
            </div>
            <div className={`text-[11px] font-bold tabular-nums mt-2 ${openColor}`}>
              {openGain ? "▲" : "▼"} {formatPct(Math.abs(p.percent_pnl))}
              <span className="mx-2 text-muted-foreground/40 font-normal">·</span>
              <span className="text-muted-foreground font-normal">Live mid-price</span>
            </div>
          </div>
        </div>

        {/* Divider */}
        <div className="relative h-px bg-gradient-to-r from-transparent via-border to-transparent mb-8" />

        {/* Lifetime block: dynamic label/number + interactive chart.
            Grid columns are FIXED so the hero number can swap between widths
            during hover scrubbing without pushing the chart horizontally. */}
        <div className="grid grid-cols-1 md:grid-cols-[440px_1fr] gap-6 md:gap-8 items-start">
          <div className="md:w-[440px] overflow-hidden">
            {/* Label line — animates between default and hover state */}
            <div className="text-[10px] uppercase tracking-[0.25em] text-muted-foreground mb-1 h-3.5 whitespace-nowrap">
              <span key={shownLabel} className="inline-block animate-[fadeIn_180ms_ease-out]">
                {shownLabel}
              </span>
            </div>
            {/* Hero number — whitespace-nowrap locks the line so it can't
                wrap mid-number when the column is tight. The column width is
                fixed above so the chart to the right never reflows. */}
            <div className={`text-5xl md:text-6xl font-black tabular-nums leading-[0.9] tracking-tight whitespace-nowrap ${shownColor} transition-colors duration-150`}>
              {shownGain ? "+" : "−"}${Math.abs(shownValue).toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}
            </div>
            {/* Sub-line reserved at fixed height so vertical stacking stays stable
                between default and scrubbing states. */}
            <div className="text-[11px] text-muted-foreground mt-2 tabular-nums min-h-[1.5em]">
              {rangePts && !rangePts.sameIdx ? (
                <span className={shownColor}>
                  {shownGain ? "▲" : "▼"}{" "}
                  {rangePct != null
                    ? `${formatPct(Math.abs(rangePct))}`
                    : "—"}
                  <span className="mx-2 text-muted-foreground/40">·</span>
                  <span className="text-muted-foreground font-normal">{rangeDuration} elapsed</span>
                </span>
              ) : rangePts?.sameIdx ? (
                <>Base anchored · drag to measure change</>
              ) : hoverPoint ? (
                <>Scrubbing · release to resume</>
              ) : (
                <>Lifetime net · Polymarket{sinceLabel && <> · since {sinceLabel}</>}</>
              )}
            </div>
            <div className="text-[10px] text-muted-foreground/70 mt-1 tabular-nums min-h-[1.2em]">
              {rangePts ? (
                <button
                  onClick={() => setRange(null)}
                  className="text-muted-foreground/80 hover:text-foreground underline underline-offset-2 decoration-dotted cursor-pointer"
                >
                  clear selection
                </button>
              ) : (
                !hoverPoint && (p.resolved_count ?? 0) > 0 && (
                  <>
                    {p.resolved_count} settled
                    {p.resolved_losses ? <> · sunk {formatUsd(p.resolved_losses)}</> : null}
                  </>
                )
              )}
            </div>
          </div>

          {/* Chart column: selector above, chart below, scale rail under */}
          <div className="min-w-0">
            <div className="flex items-center justify-end mb-3">
              <TimeframeSelector
                value={timeframe}
                onChange={setTimeframe}
                gain={defaultValue >= 0}
              />
            </div>
            <div className="h-[140px] min-w-0">
              <PnlSparkline
                series={series}
                gain={defaultValue >= 0}
                hoverIndex={hoverIndex}
                onHoverChange={setHoverIndex}
                range={range}
                onRangeChange={setRange}
              />
            </div>
            {/* Window scale rail: start, midline marker, end */}
            {windowStart && windowEnd && (
              <div className="flex items-center justify-between text-[9px] text-muted-foreground/70 tabular-nums mt-1 px-[6px] font-mono">
                <span>{fmtShort(windowStart)}</span>
                <span className="flex-1 mx-3 h-px bg-border/40" />
                <span>{fmtShort(windowEnd)}</span>
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  )
}

function TopMoversRow({ positions }: { positions: Position[] }) {
  if (positions.length === 0) return null
  const sorted = [...positions].sort((a, b) => b.cash_pnl - a.cash_pnl)
  const gainer = sorted[0]
  const loser = sorted[sorted.length - 1]
  // Only show if there's a meaningful delta
  if (gainer.cash_pnl <= 0 && loser.cash_pnl >= 0) return null

  const Card = ({ label, pos, gain }: { label: string; pos: Position; gain: boolean }) => {
    const color = gain ? "text-emerald-700" : "text-[#b53333]"
    const bg = gain ? "bg-emerald-500/5 border-emerald-500/20" : "bg-[#b53333]/5 border-red-300/20"
    return (
      <div className={`relative overflow-hidden rounded-xl border ${bg} p-4`}>
        <div className="flex items-center justify-between mb-3">
          <span className="text-[10px] uppercase tracking-[0.25em] text-muted-foreground">{label}</span>
          <span className={`text-[10px] font-bold uppercase tracking-wider ${color}`}>
            {gain ? "▲" : "▼"} {formatPct(Math.abs(pos.percent_pnl))}
          </span>
        </div>
        <div className="flex items-start gap-3">
          {pos.icon && <img src={pos.icon} alt="" className="w-9 h-9 rounded object-cover shrink-0" />}
          <div className="min-w-0 flex-1">
            <div className="text-sm font-semibold truncate">{cleanMatchupTitle(pos.title)}</div>
            <div className="text-[11px] text-muted-foreground truncate mt-0.5">{pos.outcome}</div>
          </div>
          <div className={`text-xl font-black tabular-nums leading-none ${color}`}>
            {formatUsdCompact(pos.cash_pnl)}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {gainer.cash_pnl > 0 && <Card label="Top Gainer" pos={gainer} gain={true} />}
      {loser.cash_pnl < 0 && <Card label="Top Loser" pos={loser} gain={false} />}
    </div>
  )
}

function PriceProgressionBar({ avg, cur, gain }: { avg: number; cur: number; gain: boolean }) {
  // Show positions of avg and cur on a 0-1 probability track.
  const avgPct = Math.max(0, Math.min(100, avg * 100))
  const curPct = Math.max(0, Math.min(100, cur * 100))
  const left = Math.min(avgPct, curPct)
  const width = Math.max(0.5, Math.abs(curPct - avgPct))
  const barColor = gain ? "bg-emerald-500/60" : "bg-[#b53333]/60"
  const curColor = gain ? "bg-emerald-600" : "bg-[#b53333]"
  return (
    <div className="relative h-1 rounded-full bg-secondary/60 overflow-visible">
      {/* Segment between avg and current */}
      <div
        className={`absolute inset-y-0 ${barColor} rounded-full`}
        style={{ left: `${left}%`, width: `${width}%` }}
      />
      {/* Avg tick */}
      <div
        className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-0.5 h-2.5 rounded-full bg-foreground/50"
        style={{ left: `${avgPct}%` }}
      />
      {/* Current marker (larger) */}
      <div
        className={`absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-2 h-2 rounded-full ring-2 ring-background ${curColor}`}
        style={{ left: `${curPct}%` }}
      />
    </div>
  )
}

function PositionRow({ pos }: { pos: Position }) {
  const gain = pos.cash_pnl >= 0
  const pnlColor = gain ? "text-emerald-700" : "text-[#b53333]"
  const type = inferMarketType(pos.title, pos.outcome)

  return (
    <div className="relative px-5 py-4 border-t border-border/30 hover:bg-secondary/10 transition-colors group">
      {/* Gain/loss accent stripe on the left edge */}
      <div className={`absolute left-0 top-0 bottom-0 w-px ${gain ? "bg-emerald-600/40" : "bg-[#b53333]/40"} group-hover:w-0.5 transition-all`} />

      <div className="flex items-center gap-4">
        {/* Left: type + outcome */}
        <div className="w-[180px] shrink-0 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <MarketTypeBadge type={type} />
            {pos.is_live && (
              <span className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-emerald-700/70">
                <span className="w-1 h-1 rounded-full bg-emerald-600 animate-pulse" />
                Live
              </span>
            )}
          </div>
          <div className="text-sm font-semibold truncate text-foreground">{pos.outcome}</div>
          <div className="text-[10px] text-muted-foreground tabular-nums mt-0.5">
            {pos.size.toLocaleString(undefined, { maximumFractionDigits: 0 })} shares
          </div>
        </div>

        {/* Middle: price progression bar */}
        <div className="flex-1 min-w-0 pr-2">
          <div className="flex items-center gap-3 text-[11px] tabular-nums mb-2">
            <span className="text-muted-foreground">
              AVG <span className="text-foreground font-semibold">{formatPrice(pos.avg_price)}</span>
            </span>
            <span className="flex-1 h-px bg-border/50" />
            <span className="text-muted-foreground">
              NOW <span className={`font-semibold ${gain ? "text-emerald-700" : "text-[#b53333]"}`}>{formatPrice(pos.cur_price)}</span>
            </span>
          </div>
          <PriceProgressionBar avg={pos.avg_price} cur={pos.cur_price} gain={gain} />
        </div>

        {/* Right: value + P/L stack */}
        <div className="w-[120px] shrink-0 text-right">
          <div className="text-base font-bold tabular-nums leading-none">
            {formatUsd(pos.current_value)}
          </div>
          <div className={`text-xs font-semibold tabular-nums mt-1.5 ${pnlColor}`}>
            {gain ? "+" : ""}{formatUsdCompact(pos.cash_pnl)}
            <span className="opacity-70 ml-1">{formatPct(pos.percent_pnl)}</span>
          </div>
        </div>
      </div>
    </div>
  )
}

function EventGroupCard({ group }: { group: EventGroup }) {
  const gain = group.totalPnl >= 0
  const pnlColor = gain ? "text-emerald-700" : "text-[#b53333]"
  const href = group.eventSlug ? `https://polymarket.com/event/${group.eventSlug}` : undefined

  return (
    <div className="overflow-hidden rounded-xl border border-border/60 bg-card/40 backdrop-blur-sm">
      {/* Group header */}
      <div className="relative px-5 py-4 bg-gradient-to-r from-card/80 via-card/60 to-card/30 border-b border-border/40">
        <div className="flex items-center gap-3">
          {group.icon && (
            <div className="relative shrink-0">
              <img src={group.icon} alt="" className="w-10 h-10 rounded-lg object-cover ring-1 ring-border/60" />
              <div className={`absolute -inset-px rounded-lg ring-1 ${gain ? "ring-emerald-400/20" : "ring-red-300/20"}`} />
            </div>
          )}
          <div className="flex-1 min-w-0">
            {href ? (
              <a href={href} target="_blank" rel="noreferrer" className="text-sm font-bold truncate hover:text-primary transition-colors block">
                {group.title}
              </a>
            ) : (
              <div className="text-sm font-bold truncate">{group.title}</div>
            )}
            <div className="text-[10px] text-muted-foreground tabular-nums mt-0.5">
              {group.positions.length} position{group.positions.length === 1 ? "" : "s"}
              <span className="mx-1.5 text-foreground/20">·</span>
              {formatUsd(group.totalValue)} value
              <span className="mx-1.5 text-foreground/20">·</span>
              Cost {formatUsd(group.totalCost)}
            </div>
          </div>
          <div className="text-right shrink-0">
            <div className={`text-lg font-black tabular-nums leading-none ${pnlColor}`}>
              {gain ? "+" : ""}{formatUsdCompact(group.totalPnl)}
            </div>
            <div className={`text-[10px] font-semibold tabular-nums mt-1 ${pnlColor}`}>
              {gain ? "▲" : "▼"} {formatPct(Math.abs(group.percentPnl))}
            </div>
          </div>
        </div>
      </div>

      {/* Positions within this event */}
      <div>
        {group.positions.map(pos => (
          <PositionRow key={pos.asset ?? pos.condition_id ?? pos.title} pos={pos} />
        ))}
      </div>
    </div>
  )
}

function PositionsBoard({ positions }: { positions: Position[] }) {
  if (positions.length === 0) {
    return (
      <div className="text-center py-16 text-sm text-muted-foreground rounded-xl border border-dashed border-border/50">
        No open positions
      </div>
    )
  }
  const groups = groupPositionsByEvent(positions)
  return (
    <div className="space-y-3">
      <div className="flex items-baseline justify-between pb-1">
        <h2 className="text-[10px] uppercase tracking-[0.3em] text-muted-foreground">
          Open Positions
        </h2>
        <span className="text-[10px] text-muted-foreground tabular-nums">
          {groups.length} event{groups.length === 1 ? "" : "s"}
          <span className="mx-1.5 text-foreground/20">·</span>
          {positions.length} position{positions.length === 1 ? "" : "s"}
        </span>
      </div>
      <div className="space-y-3">
        {groups.map(g => <EventGroupCard key={g.key} group={g} />)}
      </div>
    </div>
  )
}

function PortfolioPage() {
  const { username } = useParams()
  const user = username ?? "whycantilose"
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    fetch(`/api/portfolio/${user}`, { signal: controller.signal })
      .then(r => r.json()).then(setPortfolio).catch(() => {})
    const es = new EventSource(`/api/portfolio/${user}/stream`)
    es.onmessage = (e) => setPortfolio(JSON.parse(e.data))
    return () => { controller.abort(); es.close() }
  }, [user])

  if (!portfolio) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-[1200px] mx-auto p-4 md:p-6">
          <Link to="/" className="text-sm text-muted-foreground hover:text-foreground mb-4 inline-block">&larr; All Games</Link>
          <div className="text-center py-20 text-muted-foreground text-lg">Loading portfolio...</div>
        </div>
      </div>
    )
  }

  if (!portfolio.available) {
    return (
      <div className="min-h-screen bg-background">
        <div className="max-w-[1200px] mx-auto p-4 md:p-6">
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
      <div className="max-w-[1200px] mx-auto p-4 md:p-6 space-y-5">
        {/* Breadcrumb rail */}
        <div className="flex items-center justify-between">
          <Link to="/" className="inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors">
            <span>←</span> All Games
          </Link>
          {portfolio.updated_at && (
            <span className="text-[10px] text-muted-foreground tabular-nums">
              Updated {new Date(portfolio.updated_at).toLocaleTimeString()}
            </span>
          )}
        </div>

        <PortfolioHero p={portfolio} user={user} />

        <TopMoversRow positions={portfolio.positions} />

        <PositionsBoard positions={portfolio.positions} />
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
