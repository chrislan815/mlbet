export interface GameInfo {
  game_pk: number
  slug: string | null
  status: string
  home_team_name: string
  away_team_name: string
  home_score: number
  away_score: number
  current_inning: number
  inning_state: string
  venue_name: string
  game_datetime?: string
}

export interface LinescoreInning {
  inning: number
  away: number
  home: number
}

export interface CurrentAB {
  atBatIndex: number
  batter_name: string
  batter_id: number
  pitcher_name: string
  pitcher_id: number
  bat_side: string
  pitch_hand: string
  is_complete: boolean
  result: string | null
  result_description: string | null
}

export interface Pitch {
  num: number
  type_code: string
  type_desc: string
  speed: number | null
  call: string
  call_code: string
  pX: number | null
  pZ: number | null
  szTop: number | null
  szBottom: number | null
  is_strike: boolean
  is_ball: boolean
  is_in_play: boolean
  spin_rate: number | null
  break_vert: number | null
  break_horiz: number | null
  hit_speed: number | null
  hit_angle: number | null
  hit_distance: number | null
}

export interface Runner {
  id: number
  name: string
}

export interface Runners {
  first: Runner | null
  second: Runner | null
  third: Runner | null
}

export interface Count {
  balls: number
  strikes: number
  outs: number
}

export interface Play {
  atBatIndex: number
  inning: number
  is_top: boolean
  event: string
  description: string
  batter: string
  pitcher: string
  away_score: number
  home_score: number
  is_scoring: boolean
}

export interface GameState {
  game: GameInfo
  linescore: LinescoreInning[]
  current_ab: CurrentAB | null
  pitches: Pitch[]
  runners: Runners
  count: Count
  plays: Play[]
}

export interface MarketOutcome {
  name: string
  price: number
  token_id: string | null
}

export interface OrderBookLevel {
  price: number
  size: number
  total: number
}

export interface OrderBook {
  bid: number | null
  ask: number | null
  spread: number
  last_trade_price: number
  bids: OrderBookLevel[]
  asks: OrderBookLevel[]
}

export interface Market {
  type: "moneyline" | "spread" | "total" | "nrfi"
  question: string
  volume: string
  line: number | null
  outcomes: MarketOutcome[]
  order_books: (OrderBook | null)[] | null
}

export interface OddsData {
  available: boolean
  loading?: boolean
  event_slug?: string
  markets: Market[]
  updated_at?: string
}

export interface Position {
  asset: string | null
  condition_id: string | null
  title: string
  icon: string | null
  event_slug: string | null
  outcome: string
  size: number
  avg_price: number
  cur_price: number
  initial_value: number
  current_value: number
  cash_pnl: number
  percent_pnl: number
  realized_pnl: number
  is_live: boolean
}

export interface PnlPoint {
  t: number   // unix seconds
  p: number   // cumulative lifetime P/L at that timestamp (USDC)
}

export interface Portfolio {
  available: boolean
  loading?: boolean
  wallet?: string
  username?: string
  positions: Position[]
  total_value: number
  total_cost: number
  total_pnl: number
  percent_pnl: number
  total_realized: number
  resolved_losses?: number
  resolved_count?: number
  lifetime_pnl?: number
  pnl_series?: PnlPoint[]
  position_count: number
  updated_at?: string
}

export interface Activity {
  proxyWallet: string
  timestamp: number
  conditionId: string
  type: string
  size: number
  usdcSize: number
  transactionHash: string
  price: number
  asset: string
  side: "BUY" | "SELL"
  outcomeIndex: number
  title: string
  slug: string
  icon: string
  eventSlug: string
  outcome: string
}
