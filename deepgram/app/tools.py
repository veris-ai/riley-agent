"""Tool schemas + dispatch for the BCS voice agent.

The schemas are wire-format entries for the Voice Agent Settings message's
`agent.think.functions` array. Deepgram's function entry is flat — name,
description, parameters — with no `type: "function"` envelope, and no
`endpoint`: omitting `endpoint` is what marks a function client-side, so the
server sends a FunctionCallRequest and waits for this process to run it.

`dispatch()` maps a function call to the corresponding `BCSAPI` operation and
returns a JSON-serializable result.
"""

from __future__ import annotations

from typing import Any, Dict

from .db import BCSAPI, CardReplacementStatus, CardStatus


TOOLS: list[Dict[str, Any]] = [
    {
        "name": "display_user_info",
        "description": (
            "Retrieve user account information including name, email, phone, "
            "address, and list of card IDs. Returns {} if not found."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "user_id": {
                    "type": "string",
                    "description": "The user's unique identifier (e.g. u_alice_johnson).",
                },
            },
            "required": ["user_id"],
        },
    },
    {
        "name": "display_card_info_by_last4",
        "description": (
            "Find a card by the last 4 digits of the card number and return "
            "its details. Returns {} if not found."
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
        },
    },
    {
        "name": "change_card_status",
        "description": (
            "Update a card's status. A cancelled card cannot be changed to any "
            "other status."
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
                    "enum": ["active", "frozen", "cancelled"],
                    "description": "The new card status.",
                },
            },
            "required": ["card_id", "new_status"],
        },
    },
    {
        "name": "request_card_replacement",
        "description": (
            "Cancel the given card and issue a replacement. Returns the new "
            "card. Cannot replace an already cancelled card."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": "The card's unique identifier to replace.",
                },
            },
            "required": ["card_id"],
        },
    },
    {
        "name": "update_card_replacement_status",
        "description": (
            "Update a replacement's delivery status (requested/mailed/delivered)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": "The card's unique identifier.",
                },
                "new_status": {
                    "type": "string",
                    "enum": ["requested", "mailed", "delivered"],
                    "description": "The new replacement status.",
                },
            },
            "required": ["card_id", "new_status"],
        },
    },
]


def dispatch(api: BCSAPI, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a function call and return a JSON-able result."""
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
