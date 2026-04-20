"""Lightweight keyword sets for extraction and classification (rules only, no ML)."""

from __future__ import annotations

# Provider / platform names (substring match, case-insensitive)
PROVIDER_HINTS: tuple[str, ...] = (
    "stripe",
    "paypal",
    "braintree",
    "adyen",
    "worldpay",
    "paynearme",
    "skrill",
    "nuvei",
    "pragmatic",
    "evolution",
    "playson",
    "relax gaming",
    "hacksaw",
    "light & wonder",
    "light and wonder",
    "igt",
    "konami",
    "aristocrat",
    "facebook",
    "google",
    "zendesk",
    "intercom",
    "freshdesk",
    "helpscout",
    "salesforce",
    "hubspot",
)

# Sweeps / social-casino / gambling-adjacent language
SWEEPS_LANGUAGE: tuple[str, ...] = (
    "sweepstakes",
    "sweeps coins",
    "sweeps cash",
    "gold coins",
    "free coins",
    "social casino",
    "no purchase necessary",
    "amusement only",
    "for entertainment purposes",
    "slot",
    "slots",
    "casino",
    "jackpot",
    "bonus wheel",
    "redeem",
    "prize",
    "sweeps coins",
)

# Payment / cashier hints
PAYMENT_PATH_HINTS: tuple[str, ...] = (
    "cashier",
    "wallet",
    "deposit",
    "withdrawal",
    "purchase",
    "payment method",
    "billing",
    "checkout",
    "credit card",
    "paypal",
    "apple pay",
    "google pay",
)

# Promoter / aggregator hints (many outbound links to operators)
PROMOTER_HINTS: tuple[str, ...] = (
    "best sweeps",
    "top sweepstakes",
    "compare",
    "reviews",
    "sweepstakes sites",
)

# Rebrand / migration phrases (also in page text)
REBRAND_PHRASES: tuple[str, ...] = (
    "redirect",
    "rebrand",
    "formerly",
    "previously",
    "moved to",
    "now at",
    "our new home",
    "we have moved",
)

# Policy / legal URL path hints
POLICY_PATH_HINTS: tuple[str, ...] = (
    "terms",
    "privacy",
    "rules",
    "sweepstakes-rules",
    "legal",
    "responsible",
    "policy",
    "cookie",
)

# Support / help
SUPPORT_PATH_HINTS: tuple[str, ...] = (
    "support",
    "help",
    "contact",
    "faq",
    "help-center",
    "helpcenter",
)
