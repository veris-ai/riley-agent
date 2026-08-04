"""Tool schemas + dispatch for the BCS voice agent.

gradbot takes tools as `ToolDef(name, description, parameters_json)` where
`parameters_json` is a JSON Schema *string*, so the schemas are declared as
dicts here and serialized once at import. `dispatch()` maps a gradbot tool
call to the corresponding `BCSAPI` operation and returns a JSON-serializable
result.

gradbot also injects its own internal `reset_asr` tool into the LLM's tool
list, but handles that call inside the multiplexer — it never reaches
`dispatch()`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import gradbot

from .db import BCSAPI, CardReplacementStatus, CardStatus


_SCHEMAS: List[Dict[str, Any]] = [
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
            "additionalProperties": False,
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
            "additionalProperties": False,
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
            "additionalProperties": False,
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
            "additionalProperties": False,
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
            "additionalProperties": False,
        },
    },
]


TOOLS: List[gradbot.ToolDef] = [
    gradbot.ToolDef(s["name"], s["description"], json.dumps(s["parameters"]))
    for s in _SCHEMAS
]


def dispatch(api: BCSAPI, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a gradbot tool call and return a JSON-able result."""
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
