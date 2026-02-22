import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _req(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing {name} in .env")
    return v

def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

@dataclass(frozen=True)
class Config:
    discord_token: str
    sellauth_api_key: str
    sellauth_shop_id: str
    autopoll_seconds: int
    default_product_channel_id: int
    default_ticket_channel_id: int
    default_payment_channel_id: int

def load_config() -> Config:
    return Config(
        discord_token=_req("DISCORD_TOKEN"),
        sellauth_api_key=_req("SELLAUTH_API_KEY"),
        sellauth_shop_id=_req("SELLAUTH_SHOP_ID"),
        autopoll_seconds=max(30, _int("AUTOPOLL_SECONDS", 60)),
        default_product_channel_id=_int("AUTOPRODUCT_CHANNEL_ID", 0),
        default_ticket_channel_id=_int("AUTOTICKET_CHANNEL_ID", 0),
        default_payment_channel_id=_int("AUTOPAYMENT_CHANNEL_ID", 0),
    )
