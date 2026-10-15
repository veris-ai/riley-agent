import os
import uuid
from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum

import psycopg2
import psycopg2.extras
from pydantic import BaseModel, Field


########################################################
# Schema (source of truth)
########################################################

class CardType(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"
    VIRTUAL = "virtual"


class CardStatus(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    FROZEN = "frozen"


class CardReplacementStatus(str, Enum):
    REQUESTED = "requested"
    MAILED = "mailed"
    DELIVERED = "delivered"


class CardReplacement(BaseModel):
    """A replacement card and its delivery lifecycle.

    Tracks the requested → mailed → delivered progression for a card that was
    replaced. Looked up by the original card (``card_id``) or by the
    replacement card's ``new_last4``.
    """

    id: str
    card_id: str
    new_last4: Optional[str] = None
    reason: Optional[str] = None
    status: CardReplacementStatus
    estimated_delivery: Optional[str] = None
    created_at: str
    updated_at: str


class Card(BaseModel):
    """Consolidated card object.

    ``request_card_replacement`` cancels the old card, issues a new one, and
    records a ``CardReplacement`` to track delivery. Any in-flight or recently
    completed replacement is attached as ``replacement`` on lookup.
    """

    id: str
    user_id: str
    name: str
    last4: str
    type: CardType
    status: CardStatus
    created_at: str
    updated_at: str
    replacement: Optional[CardReplacement] = None


class User(BaseModel):
    id: str
    name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None
    cards: List[str] = Field(default_factory=list)


########################################################
# PostgreSQL wrapper
########################################################

def _get_dsn() -> str:
    return os.getenv("DATABASE_URL")


def _conn():
    conn = psycopg2.connect(_get_dsn(), cursor_factory=psycopg2.extras.RealDictCursor)
    conn.set_client_encoding('UTF8')
    return conn


def _ts(val) -> str:
    """Convert a datetime/string to ISO string."""
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


class Database:
    @staticmethod
    def now_iso() -> str:
        return datetime.utcnow().isoformat() + "Z"

    def get_user_by_id(self, user_id: str) -> Optional[Dict]:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            user = dict(row)
            cur.execute("SELECT id FROM cards WHERE user_id = %s", (user_id,))
            user["cards"] = [r["id"] for r in cur.fetchall()]
            return user

    def get_card_by_id(self, card_id: str) -> Optional[Dict]:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM cards WHERE id = %s", (card_id,))
            row = cur.fetchone()
            if not row:
                return None
            card = dict(row)
            card["created_at"] = _ts(card["created_at"])
            card["updated_at"] = _ts(card["updated_at"])
            return card

    def find_card_by_last4(self, last4: str) -> Optional[Dict]:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM cards WHERE last4 = %s LIMIT 1", (last4,))
            row = cur.fetchone()
            if not row:
                return None
            card = dict(row)
            card["created_at"] = _ts(card["created_at"])
            card["updated_at"] = _ts(card["updated_at"])
            return card

    def update_card_status(self, card_id: str, new_status: str) -> Optional[Dict]:
        now = self.now_iso()
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE cards SET status = %s, updated_at = %s WHERE id = %s RETURNING *",
                (new_status, now, card_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            card = dict(row)
            card["created_at"] = _ts(card["created_at"])
            card["updated_at"] = _ts(card["updated_at"])
            return card

    def create_card(self, card: Dict) -> Dict:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO cards (id, user_id, name, last4, type, status, created_at, updated_at)
                   VALUES (%(id)s, %(user_id)s, %(name)s, %(last4)s, %(type)s, %(status)s, %(created_at)s, %(updated_at)s)
                   RETURNING *""",
                card,
            )
            row = cur.fetchone()
            result = dict(row)
            result["created_at"] = _ts(result["created_at"])
            result["updated_at"] = _ts(result["updated_at"])
            return result

    # --------------------------
    # Replacement persistence
    # --------------------------
    @staticmethod
    def _replacement_row(row) -> Optional[Dict]:
        if not row:
            return None
        rep = dict(row)
        rep["created_at"] = _ts(rep["created_at"])
        rep["updated_at"] = _ts(rep["updated_at"])
        return rep

    def get_replacement_for_card(self, card_id: str) -> Optional[Dict]:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM replacements WHERE card_id = %s ORDER BY created_at DESC LIMIT 1",
                (card_id,),
            )
            return self._replacement_row(cur.fetchone())

    def find_replacement_by_new_last4(self, last4: str) -> Optional[Dict]:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM replacements WHERE new_last4 = %s ORDER BY created_at DESC LIMIT 1",
                (last4,),
            )
            return self._replacement_row(cur.fetchone())

    def create_replacement(self, replacement: Dict) -> Dict:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO replacements
                       (id, card_id, new_last4, reason, status, estimated_delivery, created_at, updated_at)
                   VALUES
                       (%(id)s, %(card_id)s, %(new_last4)s, %(reason)s, %(status)s, %(estimated_delivery)s, %(created_at)s, %(updated_at)s)
                   RETURNING *""",
                replacement,
            )
            return self._replacement_row(cur.fetchone())

    def update_replacement_status(self, replacement_id: str, new_status: str) -> Optional[Dict]:
        now = self.now_iso()
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE replacements SET status = %s, updated_at = %s WHERE id = %s RETURNING *",
                (new_status, now, replacement_id),
            )
            return self._replacement_row(cur.fetchone())


########################################################
# API layer (validated facade over Database)
########################################################

class BCSAPI(BaseModel):
    """High-level API used by agents/tools.

    Performs validation using the `User` and `Card` schemas and delegates
    persistence to `Database`. Returns plain dicts to match tool expectations.
    """

    db: Database = Field(default_factory=Database)

    # Allow non-pydantic types like Database inside this model
    model_config = {
        "arbitrary_types_allowed": True,
    }

    # --------------------------
    # User operations
    # --------------------------
    def get_user_info(self, user_id: str) -> User:
        doc = self.db.get_user_by_id(user_id)
        return User(**doc) if doc else None

    # --------------------------
    # Card operations
    # --------------------------
    def _attach_replacement(self, card_doc: Dict) -> Dict:
        """Attach any in-flight/recent replacement to a card dict, looked up by
        the original card id or by the replacement card's last4."""
        rep = self.db.get_replacement_for_card(card_doc["id"])
        if rep is None:
            rep = self.db.find_replacement_by_new_last4(card_doc["last4"])
        card_doc["replacement"] = rep
        return card_doc

    def find_card_by_last4(self, last4: str) -> Card:
        doc = self.db.find_card_by_last4(last4)
        return Card(**self._attach_replacement(doc)) if doc else None

    def update_card_status(self, card_id: str, new_status: CardStatus) -> Card:
        current = self.db.get_card_by_id(card_id)
        if not current:
            raise ValueError("card not found")
        if current.get("status") == CardStatus.CANCELLED and new_status != CardStatus.CANCELLED:
            raise ValueError("cannot change status of a cancelled card")
        updated = self.db.update_card_status(card_id, new_status)
        if updated is None:
            raise ValueError("card not found")
        return Card(**updated)

    def request_card_replacement(self, card_id: str, reason: Optional[str] = None) -> Card:
        old = self.db.get_card_by_id(card_id)
        if not old:
            raise ValueError("card not found")
        if old.get("status") == "cancelled":
            raise ValueError("cannot replace an already cancelled card")

        # Cancel the old card
        self.update_card_status(card_id, CardStatus.CANCELLED)

        # Create successor card
        now = datetime.utcnow().isoformat() + "Z"
        new_id = f"c_{uuid.uuid4().hex[:8]}"
        new_last4 = f"{uuid.uuid4().int % 10000:04d}"
        successor = Card(
            id=new_id,
            user_id=old["user_id"],
            name=f"{old['name']} (replacement)",
            last4=new_last4,
            type=old["type"],
            status=CardStatus.ACTIVE,
            created_at=now,
            updated_at=now,
        )

        self.db.create_card(successor.model_dump(exclude={"replacement"}))

        # Record the replacement so its delivery (requested → mailed →
        # delivered) can be tracked and reported back to the caller.
        replacement = CardReplacement(
            id=f"rep_{uuid.uuid4().hex[:8]}",
            card_id=card_id,
            new_last4=new_last4,
            reason=reason,
            status=CardReplacementStatus.REQUESTED,
            estimated_delivery="14 business days",
            created_at=now,
            updated_at=now,
        )
        self.db.create_replacement(replacement.model_dump())
        successor.replacement = replacement
        return successor

    def update_card_replacement_status(
        self, card_id: str, new_status: CardReplacementStatus
    ) -> CardReplacement:
        """Advance a replacement's delivery status (requested → mailed → delivered).

        Accepts either the original card's id or the replacement card's id —
        the caller may reference whichever card they have in hand.
        """
        rep = self.db.get_replacement_for_card(card_id)
        if rep is None:
            card = self.db.get_card_by_id(card_id)
            if card:
                rep = self.db.find_replacement_by_new_last4(card["last4"])
        if rep is None:
            raise ValueError("no replacement found for this card")
        updated = self.db.update_replacement_status(rep["id"], new_status)
        return CardReplacement(**updated)
