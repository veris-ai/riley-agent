"""Tool schemas + dispatch for the BCS Retell voice agent.

`TOOL_FUNCTIONS` defines the OpenAI-function shape shared by every
Riley variant. `build_tools()` wraps each entry as a Retell LLM
`general_tools` custom function pointing at our `/tool` webhook, so
Retell cloud knows where to POST mid-call tool requests.

`dispatch()` exposes the same five card-ops tools as the other Riley
transports — same BCSAPI calls — and takes `(name, args)` matching the `{name, call, args}` envelope
Retell delivers to the webhook.
"""

from __future__ import annotations

from typing import Any, Dict, List

from .db import BCSAPI, CardReplacementStatus, CardStatus


TOOL_FUNCTIONS: List[Dict[str, Any]] = [
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


def build_tools(server_url: str) -> List[Dict[str, Any]]:
    """Wrap each TOOL_FUNCTIONS entry as a Retell custom function that POSTs to ``server_url``."""
    return [
        {
            "type": "custom",
            "name": fn["name"],
            "description": fn["description"],
            "url": server_url,
            "parameters": fn["parameters"],
            "speak_during_execution": False,
            "speak_after_execution": True,
        }
        for fn in TOOL_FUNCTIONS
    ]


def dispatch(api: BCSAPI, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a Retell tool call and return a JSON-able result."""
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
        return rep.model_dump() if rep else {}

    raise ValueError(f"unknown tool: {name}")
