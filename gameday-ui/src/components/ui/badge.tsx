import { cn } from "@/lib/utils"
import type { HTMLAttributes } from "react"

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "live" | "final" | "secondary"
}

export function Badge({ className, variant = "default", ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-2 py-0.5 text-xs font-bold uppercase tracking-wide",
        variant === "live" && "bg-emerald-500 text-black animate-pulse",
        variant === "final" && "bg-zinc-500 text-white",
        variant === "secondary" && "bg-secondary text-secondary-foreground",
        variant === "default" && "bg-primary text-primary-foreground",
        className,
      )}
      {...props}
    />
  )
}
