"""Tool schemas + dispatch for the BCS voice agent, Fluxions realtime flavour.

`dispatch()` is identical in behaviour to the other Riley rows — the five
card-ops operations against `BCSAPI`.

The *descriptions* are not identical, and that is deliberate. Fluxions routes
tool calls by JSON-mode prompting rather than the OpenAI tools API, so a
description is the only instruction the router reads — it does not see the
conversation the way a function-calling model does. A description therefore has
to say *when* to call the tool, not just what it does. Each one here leads with
its trigger condition and names which identifiers come from which earlier call,
since a mutation's `card_id` can only have come from a previous lookup.

The tool *set* is unchanged — the same five operations every other Riley row
gets — so this row is scored on the same surface as the rest.
"""

from __future__ import annotations

from typing import Any, Dict, List

from .db import BCSAPI, CardReplacementStatus, CardStatus


_SCHEMAS: List[Dict[str, Any]] = [
    {
        "name": "display_card_info_by_last4",
        "description": (
            "Look up a card by the last 4 digits of the card number. Call this "
            "immediately whenever the caller says four digits, including when "
            "they are giving them to verify their identity. Returns the card's "
            "id, status, type and owning user_id — you need the id before any "
            "change. Returns {} if not found."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "last4": {
                    "type": "string",
                    "description": "The last 4 digits of the card number (e.g. '1234').",
                },
            },
            "required": ["last4"],
            "additionalProperties": False,
        },
    },
    {
        "name": "display_user_info",
        "description": (
            "Retrieve a user's account details: name, email, phone, address and "
            "card ids. Call this whenever you need to check what a caller told "
            "you against the account — their name, address or phone — or when "
            "they ask which cards they have. Returns {} if not found."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "user_id": {
                    "type": "string",
                    "description": (
                        "The user's unique identifier (e.g. u_alice_johnson), as "
                        "returned by display_card_info_by_last4."
                    ),
                },
            },
            "required": ["user_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "change_card_status",
        "description": (
            "Freeze, unfreeze, or cancel a card. Call this whenever the caller "
            "asks to freeze, lock, unfreeze, or cancel a card, once you have "
            "looked the card up and have its id. A cancelled card cannot be "
            "changed to any other status."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": (
                        "The card's unique identifier (e.g. c_alice_debit), as "
                        "returned by display_card_info_by_last4. Never the last 4 digits."
                    ),
                },
                "new_status": {
                    "type": "string",
                    "enum": ["active", "frozen", "cancelled"],
                    "description": "The new card status.",
                },
            },
            "required": ["card_id", "new_status"],
            "additionalProperties": False,
        },
    },
    {
        "name": "request_card_replacement",
        "description": (
            "Cancel the given card and issue a replacement. Call this whenever "
            "the caller asks for a new or replacement card — because theirs was "
            "lost, stolen or damaged — once you have looked the card up and have "
            "its id. Returns the new card. Cannot replace an already cancelled card."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": (
                        "The unique identifier of the card to replace (e.g. "
                        "c_alice_debit). Never the last 4 digits."
                    ),
                },
            },
            "required": ["card_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "update_card_replacement_status",
        "description": (
            "Update a replacement's delivery status (requested/mailed/delivered). "
            "Call this whenever the caller reports the new card arrived, asks you "
            "to move a replacement along, or wants a delivered card activated."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": "The card's unique identifier (e.g. c_alice_debit).",
                },
                "new_status": {
                    "type": "string",
                    "enum": ["requested", "mailed", "delivered"],
                    "description": "The new replacement status.",
                },
            },
            "required": ["card_id", "new_status"],
            "additionalProperties": False,
        },
    },
]


# Fluxions accepts both the flat and the nested shape; the nested one matches
# what the other rows send.
TOOLS: List[Dict[str, Any]] = [{"type": "function", "function": s} for s in _SCHEMAS]


def dispatch(api: BCSAPI, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one tool call and return a JSON-able result."""
    if name == "display_user_info":
        user = api.get_user_info(args["user_id"])
        return user.model_dump() if user else {}

    if name == "display_card_info_by_last4":
        card = api.find_card_by_last4(args["last4"])
        return card.model_dump() if card else {}

    if name == "change_card_status":
        card = api.update_card_status(args["card_id"], CardStatus(args["new_status"]))
        return card.model_dump() if card else {}

    if name == "request_card_replacement":
        card = api.request_card_replacement(args["card_id"])
        return card.model_dump() if card else {}

    if name == "update_card_replacement_status":
        rep = api.update_card_replacement_status(
            args["card_id"], CardReplacementStatus(args["new_status"])
        )
        return rep.model_dump()

    raise ValueError(f"unknown tool: {name}")
