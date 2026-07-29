"""Tool schemas + dispatch for the BCS voice agent (ElevenLabs flavor).

The schemas are wire-format entries for the ElevenLabs agent's
`conversation_config.agent.prompt.tools` array (type=`client`). `dispatch()`
maps a `client_tool_call` to the corresponding `BCSAPI` operation and returns
a JSON-serializable result.
"""

from __future__ import annotations

from typing import Any, Dict

from .db import BCSAPI, CardReplacementStatus, CardStatus


TOOLS: list[Dict[str, Any]] = [
    {
        "type": "client",
        "name": "display_user_info",
        "description": (
            "Retrieve user account information including name, email, phone, "
            "address, and list of card IDs. Returns {} if not found."
        ),
        "parameters": {
            "type": "object",
            "required": ["user_id"],
            "properties": {
                "user_id": {
                    "type": "string",
                    "description": "The user's unique identifier (e.g. u_alice_johnson).",
                },
            },
        },
        "expects_response": True,
    },
    {
        "type": "client",
        "name": "display_card_info_by_last4",
        "description": (
            "Find a card by the last 4 digits of the card number and return "
            "its details. Returns {} if not found."
        ),
        "parameters": {
            "type": "object",
            "required": ["last4"],
            "properties": {
                "last4": {
                    "type": "string",
                    "description": "The last 4 digits of the card number (e.g. '1234').",
                },
            },
        },
        "expects_response": True,
    },
    {
        "type": "client",
        "name": "change_card_status",
        "description": (
            "Update a card's status. A cancelled card cannot be changed to any "
            "other status."
        ),
        "parameters": {
            "type": "object",
            "required": ["card_id", "new_status"],
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": "The card's unique identifier (e.g. c_alice_debit).",
                },
                "new_status": {
                    "type": "string",
                    "description": "The new card status.",
                    "enum": ["active", "frozen", "cancelled"],
                },
            },
        },
        "expects_response": True,
    },
    {
        "type": "client",
        "name": "request_card_replacement",
        "description": (
            "Cancel the given card and issue a replacement. Returns the new "
            "card. Cannot replace an already cancelled card."
        ),
        "parameters": {
            "type": "object",
            "required": ["card_id"],
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": "The card's unique identifier to replace.",
                },
            },
        },
        "expects_response": True,
    },
    {
        "type": "client",
        "name": "update_card_replacement_status",
        "description": (
            "Update a replacement's delivery status (requested/mailed/delivered)."
        ),
        "parameters": {
            "type": "object",
            "required": ["card_id", "new_status"],
            "properties": {
                "card_id": {
                    "type": "string",
                    "description": "The card's unique identifier.",
                },
                "new_status": {
                    "type": "string",
                    "description": "The new replacement status.",
                    "enum": ["requested", "mailed", "delivered"],
                },
            },
        },
        "expects_response": True,
    },
]


def dispatch(api: BCSAPI, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute an ElevenLabs client_tool_call and return a JSON-able result."""
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
