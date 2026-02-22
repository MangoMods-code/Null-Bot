import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

from configs import load_config, Config

logging.basicConfig(level=logging.INFO)

# ============================================================
# DB
# ============================================================
class KVDB:
    def __init__(self, path: str = "state.db") -> None:
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self.conn.commit()

    def get(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM kv WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO kv(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_json(self, key: str) -> Any:
        raw = self.get(key)
        return json.loads(raw) if raw else None

    def set_json(self, key: str, obj: Any) -> None:
        self.set(key, json.dumps(obj, separators=(",", ":")))


# ============================================================
# SellAuth HTTP (GET + PATCH)
# ============================================================
class SellAuthHTTP:
    BASE_URL = "https://api.sellauth.com"  # may vary by SellAuth API version

    def __init__(self, api_key: str, shop_id: str) -> None:
        self.api_key = api_key
        self.shop_id = shop_id

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _get(self, session: aiohttp.ClientSession, path: str) -> Any:
        url = f"{self.BASE_URL}{path}"
        async with session.get(url, headers=self._headers()) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"SellAuth GET {path} failed {r.status}: {text[:300]}")
            try:
                return json.loads(text)
            except Exception:
                return text

    async def _patch(self, session: aiohttp.ClientSession, path: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.BASE_URL}{path}"
        async with session.patch(url, headers=self._headers(), json=payload) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"SellAuth PATCH {path} failed {r.status}: {text[:300]}")
            try:
                return json.loads(text)
            except Exception:
                return text

    async def list_products(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        data = await self._get(session, f"/v1/shops/{self.shop_id}/products")
        return data.get("data", data) if isinstance(data, dict) else data

    async def list_tickets(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        data = await self._get(session, f"/v1/shops/{self.shop_id}/tickets")
        return data.get("data", data) if isinstance(data, dict) else data

    async def list_payments(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        data = await self._get(session, f"/v1/shops/{self.shop_id}/payments")
        return data.get("data", data) if isinstance(data, dict) else data

    # --- write endpoints (may need adjustment depending on SellAuth API) ---
    async def update_product(self, session: aiohttp.ClientSession, product_id: str, payload: Dict[str, Any]) -> Any:
        return await self._patch(session, f"/v1/shops/{self.shop_id}/products/{product_id}", payload)

    async def update_variant(self, session: aiohttp.ClientSession, product_id: str, variant_id: str, payload: Dict[str, Any]) -> Any:
        return await self._patch(session, f"/v1/shops/{self.shop_id}/products/{product_id}/variants/{variant_id}", payload)


# ============================================================
# Permissions
# ============================================================
STAFF_ROLE_ID = int(os.getenv("STAFF_ROLE_ID", "0") or "0")
OWNER_ROLE_ID = int(os.getenv("OWNER_ROLE_ID", "0") or "0")

def _has_role(interaction: discord.Interaction, role_id: int) -> bool:
    if not role_id or not interaction.guild:
        return False
    member = interaction.guild.get_member(interaction.user.id)
    if not member:
        return False
    return any(r.id == role_id for r in member.roles)

def is_owner(interaction: discord.Interaction) -> bool:
    if interaction.user and hasattr(interaction.user, "guild_permissions"):
        if interaction.user.guild_permissions.administrator:
            return True
    return _has_role(interaction, OWNER_ROLE_ID)

def is_staff(interaction: discord.Interaction) -> bool:
    if is_owner(interaction):
        return True
    if interaction.user and hasattr(interaction.user, "guild_permissions"):
        if interaction.user.guild_permissions.administrator:
            return True
    return _has_role(interaction, STAFF_ROLE_ID)

async def deny(interaction: discord.Interaction, msg: str = "❌ You don’t have permission to use this."):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass


# ============================================================
# Helpers
# ============================================================
def pick_id(obj: Dict[str, Any]) -> str:
    return str(obj.get("id") or obj.get("_id") or obj.get("uuid") or "")

def _deep_find_first(obj, wanted_keys: set, max_depth: int = 4):
    if max_depth <= 0:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in wanted_keys and v is not None:
                return v
        for v in obj.values():
            found = _deep_find_first(v, wanted_keys, max_depth - 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj[:10]:
            found = _deep_find_first(item, wanted_keys, max_depth - 1)
            if found is not None:
                return found
    return None

def pick_price(p: dict) -> str:
    v = _deep_find_first(p, {"price", "unit_price", "amount", "cost", "value", "usd", "eur"})
    if v is None:
        return "—"
    s = str(v).strip()
    return s if s and s.lower() != "none" else "—"

def pick_stock(p: dict) -> str:
    v = _deep_find_first(p, {"stock", "quantity", "remaining", "inventory", "in_stock", "available"})
    if v is None:
        return "—"
    s = str(v).strip()
    return s if s and s.lower() != "none" else "—"

def pick_name(p: dict) -> str:
    return str(p.get("name") or p.get("title") or "Unnamed")

def pick_status(p: dict) -> str:
    if p.get("status") is not None:
        return str(p.get("status"))
    if p.get("enabled") is not None:
        return f"enabled={p.get('enabled')}"
    if p.get("active") is not None:
        return f"active={p.get('active')}"
    return "—"

def pick_variants(p: dict) -> list:
    for key in ("variants", "options", "packages"):
        v = p.get(key)
        if isinstance(v, list) and v:
            out = []
            for item in v:
                if not isinstance(item, dict):
                    continue
                out.append({
                    "id": str(item.get("id") or item.get("_id") or item.get("uuid") or ""),
                    "name": str(item.get("name") or item.get("title") or item.get("label") or "Variant"),
                    "price": pick_price(item),
                    "stock": pick_stock(item),
                })
            return out

    for _, v in p.items():
        if isinstance(v, dict):
            for subk, subv in v.items():
                if isinstance(subv, list) and subv and isinstance(subv[0], dict):
                    probe = subv[0]
                    if any(k in probe for k in ("price", "stock", "quantity", "remaining", "amount")):
                        out = []
                        for item in subv:
                            out.append({
                                "id": str(item.get("id") or item.get("_id") or item.get("uuid") or ""),
                                "name": str(item.get("name") or item.get("title") or item.get("label") or subk),
                                "price": pick_price(item),
                                "stock": pick_stock(item),
                            })
                        return out
    return []

def normalize_variant_map(p: dict) -> Dict[str, Dict[str, str]]:
    variants = pick_variants(p)
    vmap: Dict[str, Dict[str, str]] = {}
    for v in variants:
        vkey = v.get("id") or v.get("name") or "Variant"
        vmap[str(vkey)] = {
            "name": str(v.get("name", "Variant")),
            "price": str(v.get("price", "—")),
            "stock": str(v.get("stock", "—")),
        }
    return vmap

def resolve_variant_by_key(product: Dict[str, Any], variant_key: str) -> Optional[Dict[str, str]]:
    variants = pick_variants(product)
    if not variants:
        return None

    for v in variants:
        if v.get("id") and str(v["id"]) == str(variant_key):
            return {"id": str(v["id"]), "name": str(v.get("name", "Variant"))}

    vk = str(variant_key).strip().lower()
    for v in variants:
        if str(v.get("name", "")).strip().lower() == vk:
            return {"id": str(v.get("id") or ""), "name": str(v.get("name", "Variant"))}
    return None



# ============================================================
# Bot + Setup Hook (FIXES "Synced 0 commands")
# ============================================================
cfg: Config = load_config()

GUILD_ID = int(os.getenv("GUILD_ID", "0") or "0")

intents = discord.Intents.default()
# You do NOT need message_content for slash commands; ignore that warning unless you add prefix commands.

class NullBot(commands.Bot):
    async def setup_hook(self) -> None:
        try:
            if GUILD_ID:
                guild = discord.Object(id=GUILD_ID)

                # 1) Clear old guild commands (stale copies)
                self.tree.clear_commands(guild=guild)

                # 2) Copy current in-code commands into guild (instant)
                self.tree.copy_global_to(guild=guild)

                # 3) Sync to guild
                synced = await self.tree.sync(guild=guild)
                logging.info("Synced %d GUILD commands to %s", len(synced), GUILD_ID)

            else:
                synced = await self.tree.sync()
                logging.info("Synced %d GLOBAL commands", len(synced))
        except Exception:
            logging.exception("Slash sync failed")

bot = NullBot(command_prefix="!", intents=intents)

bot.db = KVDB("state.db")
bot.sellauth = SellAuthHTTP(cfg.sellauth_api_key, cfg.sellauth_shop_id)

# Defaults (optional)
if bot.db.get("product_channel_id") is None and cfg.default_product_channel_id:
    bot.db.set("product_channel_id", str(cfg.default_product_channel_id))
if bot.db.get("ticket_channel_id") is None and cfg.default_ticket_channel_id:
    bot.db.set("ticket_channel_id", str(cfg.default_ticket_channel_id))
if bot.db.get("payment_channel_id") is None and cfg.default_payment_channel_id:
    bot.db.set("payment_channel_id", str(cfg.default_payment_channel_id))

async def get_text_channel(key: str) -> Optional[discord.TextChannel]:
    raw = bot.db.get(key)
    if not raw:
        return None
    try:
        cid = int(raw)
    except Exception:
        return None
    ch = bot.get_channel(cid)
    return ch if isinstance(ch, discord.TextChannel) else None

async def post_embed(channel_key: str, embed: discord.Embed) -> None:
    ch = await get_text_channel(channel_key)
    if ch:
        await ch.send(embed=embed)

@bot.event
async def on_ready():
    logging.info("Logged in as %s (%s)", bot.user, bot.user.id)
    if not poll_loop.is_running():
        poll_loop.change_interval(seconds=cfg.autopoll_seconds)
        poll_loop.start()











# ============================================================
# Commands: Autopost
# ============================================================
@bot.tree.command(name="autopost_set", description="Owner: set autopost channel for products/tickets/payments")
@app_commands.describe(kind="products | tickets | payments", channel="Channel to post into")
async def autopost_set(interaction: discord.Interaction, kind: str, channel: discord.TextChannel):
    if not is_owner(interaction):
        return await deny(interaction, "❌ Owner only.")

    kind = kind.lower().strip()
    mapping = {
        "products": "product_channel_id",
        "tickets": "ticket_channel_id",
        "payments": "payment_channel_id",
    }
    if kind not in mapping:
        return await interaction.response.send_message(
            "kind must be: `products`, `tickets`, or `payments`",
            ephemeral=True,
        )

    bot.db.set(mapping[kind], str(channel.id))
    await interaction.response.send_message(
        f"✅ {kind} autopost channel set to {channel.mention}", ephemeral=True
    )

@bot.tree.command(name="autopost_status", description="Show autopost configuration (staff)")
async def autopost_status(interaction: discord.Interaction):
    if not is_staff(interaction):
        return await deny(interaction)

    def fmt_channel(raw: Optional[str]) -> str:
        if not raw:
            return "`not set`"
        try:
            cid = int(raw)
        except Exception:
            return f"`{raw}`"
        return f"<#{cid}> (`{cid}`)"

    prod = bot.db.get("product_channel_id")
    tick = bot.db.get("ticket_channel_id")
    pay = bot.db.get("payment_channel_id")

    embed = discord.Embed(title="Autopost Status", color=discord.Color.red())
    embed.add_field(name="Products", value=fmt_channel(prod), inline=False)
    embed.add_field(name="Tickets", value=fmt_channel(tick), inline=False)
    embed.add_field(name="Payments", value=fmt_channel(pay), inline=False)
    embed.set_footer(text=f"Poll interval: {cfg.autopoll_seconds}s")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ============================================================
# Products pager (staff-only, auto pages, no “… more”)
# ============================================================
class ProductsPager(discord.ui.View):
    MAX_FIELDS = 25
    MAX_FIELD_VALUE = 1024

    def __init__(self, items: list):
        super().__init__(timeout=180)
        self.items = items
        self.page = 0
        self.pages = self._build_pages(items)

    def page_count(self) -> int:
        return max(1, len(self.pages))

    def _build_pages(self, items: list) -> list[list[dict]]:
        pages: list[list[dict]] = []
        current_fields: list[dict] = []

        def push_page():
            nonlocal current_fields
            if current_fields:
                pages.append(current_fields)
            current_fields = []

        def join_lines(lines: list[str]) -> str:
            return "\n".join(lines)

        for p in items:
            name = pick_name(p)
            pid = pick_id(p)
            price = pick_price(p)
            stock = pick_stock(p)

            variants = pick_variants(p)

            base_lines = [
                f"ID: `{pid}`",
                f"Price: `{price}`",
                f"Stock: `{stock}`",
            ]

            variant_lines: list[str] = []
            if variants:
                variant_lines.append("Variants:")
                for v in variants:
                    variant_lines.append(f"• **{v['name']}** — `{v['price']}` • stock `{v['stock']}`")

            # First field tries to include as many lines as possible
            first_value_lines = base_lines[:]
            remaining = variant_lines[:]

            while remaining:
                tentative = join_lines(first_value_lines + [""] + remaining[:1])
                if len(tentative) <= self.MAX_FIELD_VALUE:
                    first_value_lines.append(remaining.pop(0))
                else:
                    break

            product_fields: list[dict] = [{
                "name": name,
                "value": join_lines(first_value_lines),
                "inline": False
            }]

            # Continuation fields (no truncation; splits across fields/pages)
            while remaining:
                chunk: list[str] = []
                while remaining:
                    next_line = remaining[0]
                    tentative = join_lines(chunk + [next_line])
                    if len(tentative) <= self.MAX_FIELD_VALUE:
                        chunk.append(remaining.pop(0))
                    else:
                        break

                if not chunk and remaining:
                    # safety: ensure progress
                    chunk = [remaining.pop(0)[: self.MAX_FIELD_VALUE - 10] + "…"]

                product_fields.append({
                    "name": f"{name} (cont.)",
                    "value": join_lines(chunk),
                    "inline": False
                })

            for f in product_fields:
                if len(current_fields) >= self.MAX_FIELDS:
                    push_page()
                current_fields.append(f)

        push_page()
        return pages if pages else [[]]

    def render(self) -> discord.Embed:
        fields = self.pages[self.page] if self.pages else []
        embed = discord.Embed(
            title="Products",
            description=f"Page **{self.page + 1}/{self.page_count()}**",
            color=discord.Color.red(),
        )
        for f in fields:
            embed.add_field(name=f["name"], value=f["value"], inline=f["inline"])
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not is_staff(interaction):
            await deny(interaction)
            return False
        return True

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = (self.page - 1) % self.page_count()
        await interaction.response.edit_message(embed=self.render(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = (self.page + 1) % self.page_count()
        await interaction.response.edit_message(embed=self.render(), view=self)

@bot.tree.command(name="products", description="Browse products (staff)")
async def products(interaction: discord.Interaction):
    if not is_staff(interaction):
        return await deny(interaction)

    await interaction.response.defer(thinking=True, ephemeral=True)
    async with aiohttp.ClientSession() as session:
        try:
            items = await bot.sellauth.list_products(session)
        except Exception as e:
            return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

    if not items:
        return await interaction.followup.send("No products found.", ephemeral=True)

    view = ProductsPager(items)
    await interaction.followup.send(embed=view.render(), view=view, ephemeral=True)

@bot.tree.command(name="tickets", description="List tickets (top 10) (staff)")
async def tickets(interaction: discord.Interaction):
    if not is_staff(interaction):
        return await deny(interaction)

    await interaction.response.defer(thinking=True, ephemeral=True)
    async with aiohttp.ClientSession() as session:
        try:
            items = await bot.sellauth.list_tickets(session)
        except Exception as e:
            return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

    if not items:
        return await interaction.followup.send("No tickets found.", ephemeral=True)

    embed = discord.Embed(title="Tickets", color=discord.Color.red())
    for t in items[:10]:
        embed.add_field(
            name=str(t.get("subject", "Ticket")),
            value=f"ID: `{pick_id(t)}` • Status: `{t.get('status','unknown')}`",
            inline=False,
        )
    await interaction.followup.send(embed=embed, ephemeral=True)


# ============================================================
# Owner-only ID commands
# ============================================================
@bot.tree.command(name="product_set_price", description="Owner: set product base price")
@app_commands.describe(product_id="Product ID", new_price="New price (number)")
async def product_set_price(interaction: discord.Interaction, product_id: str, new_price: float):
    if not is_owner(interaction):
        return await deny(interaction, "❌ Owner only.")
    await interaction.response.defer(thinking=True, ephemeral=True)
    async with aiohttp.ClientSession() as session:
        try:
            await bot.sellauth.update_product(session, product_id, {"price": new_price})
        except Exception as e:
            return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)
    await interaction.followup.send(f"✅ Updated product `{product_id}` price to `{new_price}`.", ephemeral=True)

@bot.tree.command(name="product_set_enabled", description="Owner: enable/disable a product")
@app_commands.describe(product_id="Product ID", enabled="True to enable, False to disable")
async def product_set_enabled(interaction: discord.Interaction, product_id: str, enabled: bool):
    if not is_owner(interaction):
        return await deny(interaction, "❌ Owner only.")
    await interaction.response.defer(thinking=True, ephemeral=True)
    async with aiohttp.ClientSession() as session:
        try:
            await bot.sellauth.update_product(session, product_id, {"enabled": enabled})
        except Exception as e:
            return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)
    await interaction.followup.send(f"✅ Product `{product_id}` set to `enabled={enabled}`.", ephemeral=True)

@bot.tree.command(name="variant_restock", description="Owner: set stock for a product variant (by id or exact name)")
@app_commands.describe(product_id="Product ID", variant="Variant ID OR exact variant name", new_stock="New stock quantity")
async def variant_restock(interaction: discord.Interaction, product_id: str, variant: str, new_stock: int):
    if not is_owner(interaction):
        return await deny(interaction, "❌ Owner only.")
    await interaction.response.defer(thinking=True, ephemeral=True)

    async with aiohttp.ClientSession() as session:
        try:
            products = await bot.sellauth.list_products(session)
        except Exception as e:
            return await interaction.followup.send(f"Failed to fetch products: `{e}`", ephemeral=True)

        product = next((p for p in products if pick_id(p) == product_id), None)
        if not product:
            return await interaction.followup.send(f"Couldn’t find product `{product_id}`.", ephemeral=True)

        v = resolve_variant_by_key(product, variant)
        if not v:
            vs = pick_variants(product)
            preview = "\n".join([f"• {vv['name']} (id: {vv['id'] or 'no-id'})" for vv in vs[:15]]) or "—"
            return await interaction.followup.send(
                "Variant not found. Use variant **ID** or exact **name**.\n"
                f"Some variants:\n{preview}",
                ephemeral=True,
            )

        if not v["id"]:
            return await interaction.followup.send(
                f"I found **{v['name']}**, but the API didn’t include a variant ID, so I can’t patch it safely.",
                ephemeral=True,
            )

        try:
            await bot.sellauth.update_variant(session, product_id, v["id"], {"stock": int(new_stock)})
        except Exception as e:
            return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

    await interaction.followup.send(
        f"✅ Variant **{v['name']}** (`{v['id']}`) stock set to `{new_stock}`.",
        ephemeral=True,
    )


# ============================================================
# Owner Store Manager UI (Search + Paging + Dropdowns)
# ============================================================
def _filter_products(products: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
    q = (query or "").strip().lower()
    if not q:
        return products
    out = []
    for p in products:
        name = pick_name(p).lower()
        pid = pick_id(p).lower()
        if q in name or q in pid:
            out.append(p)
    return out

def _page(items: List[Any], page: int, per: int) -> Tuple[List[Any], int]:
    if per <= 0:
        per = 25
    total_pages = max(1, (len(items) + per - 1) // per)
    page = max(0, min(page, total_pages - 1))
    start = page * per
    return items[start:start + per], total_pages

class SearchModal(discord.ui.Modal, title="Search Products"):
    query = discord.ui.TextInput(
        label="Search by product name or ID",
        placeholder="e.g. 'FreeFire' or '579943'",
        required=False,
        max_length=100,
    )

    def __init__(self, view_ref: "OwnerManagerView"):
        super().__init__(timeout=300)
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        if not is_owner(interaction):
            return await deny(interaction, "❌ Owner only.")

        self.view_ref.search = str(self.query.value or "").strip()
        self.view_ref.page = 0
        self.view_ref.rebuild_product_select()

        await interaction.response.edit_message(embed=self.view_ref.make_embed(), view=self.view_ref)

class SetProductPriceModal(discord.ui.Modal, title="Set Product Price"):
    new_price = discord.ui.TextInput(
        label="New price",
        placeholder="e.g. 9.99",
        required=True,
        max_length=20,
    )

    def __init__(self, view_ref: "OwnerManagerView"):
        super().__init__(timeout=300)
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        if not is_owner(interaction):
            return await deny(interaction, "❌ Owner only.")

        prod = self.view_ref.selected_product()
        if not prod:
            return await interaction.response.send_message("Pick a product first.", ephemeral=True)

        try:
            price = float(str(self.new_price.value).strip())
        except Exception:
            return await interaction.response.send_message("Invalid price.", ephemeral=True)

        await interaction.response.defer(thinking=True, ephemeral=True)
        async with aiohttp.ClientSession() as session:
            try:
                await bot.sellauth.update_product(session, pick_id(prod), {"price": price})
            except Exception as e:
                return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

        await interaction.followup.send(f"✅ Updated **{pick_name(prod)}** price to `{price}`.", ephemeral=True)

class SetVariantStockModal(discord.ui.Modal, title="Set Variant Stock"):
    new_stock = discord.ui.TextInput(
        label="New stock",
        placeholder="e.g. 25",
        required=True,
        max_length=20,
    )

    def __init__(self, view_ref: "OwnerManagerView"):
        super().__init__(timeout=300)
        self.view_ref = view_ref

    async def on_submit(self, interaction: discord.Interaction):
        if not is_owner(interaction):
            return await deny(interaction, "❌ Owner only.")

        prod = self.view_ref.selected_product()
        var = self.view_ref.selected_variant()
        if not prod:
            return await interaction.response.send_message("Pick a product first.", ephemeral=True)
        if not var:
            return await interaction.response.send_message("Pick a variant first.", ephemeral=True)
        if not var["id"]:
            return await interaction.response.send_message("Variant has no ID in API payload.", ephemeral=True)

        try:
            stock = int(str(self.new_stock.value).strip())
        except Exception:
            return await interaction.response.send_message("Invalid stock number.", ephemeral=True)

        await interaction.response.defer(thinking=True, ephemeral=True)
        async with aiohttp.ClientSession() as session:
            try:
                await bot.sellauth.update_variant(session, pick_id(prod), var["id"], {"stock": stock})
            except Exception as e:
                return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

        await interaction.followup.send(
            f"✅ Set **{pick_name(prod)}** / **{var['name']}** stock to `{stock}`.",
            ephemeral=True,
        )

class OwnerManagerView(discord.ui.View):
    def __init__(self, owner_id: int, products_cache: List[Dict[str, Any]]):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.products_cache = products_cache

        self.search: str = ""
        self.page: int = 0

        self._selected_product_id: Optional[str] = None
        self._selected_variant_key: Optional[str] = None

        self.product_select = ProductSelect(self)
        self.variant_select = VariantSelect(self)

        self.add_item(self.product_select)
        self.add_item(self.variant_select)

        self.add_item(SearchButton())
        self.add_item(PrevPageButton())
        self.add_item(NextPageButton())
        self.add_item(RefreshButton())

        self.add_item(SetPriceButton())
        self.add_item(ToggleEnabledButton())
        self.add_item(SetVariantStockButton())

        self.rebuild_product_select()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await deny(interaction, "❌ This panel isn’t for you.")
            return False
        if not is_owner(interaction):
            await deny(interaction, "❌ Owner only.")
            return False
        return True

    def filtered_products(self) -> List[Dict[str, Any]]:
        return _filter_products(self.products_cache, self.search)

    def selected_product(self) -> Optional[Dict[str, Any]]:
        if not self._selected_product_id:
            return None
        return next((p for p in self.products_cache if pick_id(p) == self._selected_product_id), None)

    def selected_variant(self) -> Optional[Dict[str, str]]:
        prod = self.selected_product()
        if not prod or not self._selected_variant_key:
            return None
        return resolve_variant_by_key(prod, self._selected_variant_key)

    def rebuild_product_select(self) -> None:
        filtered = self.filtered_products()
        page_items, total_pages = _page(filtered, self.page, 25)
        # clamp page if needed
        self.page = max(0, min(self.page, total_pages - 1))

        self.product_select.set_options(page_items, self.page, total_pages)
        self.variant_select.refresh_options()

    def make_embed(self) -> discord.Embed:
        filtered = self.filtered_products()
        _, total_pages = _page(filtered, self.page, 25)

        prod = self.selected_product()
        var = self.selected_variant()

        embed = discord.Embed(title="Owner Store Manager", color=discord.Color.red())
        embed.add_field(
            name="Search",
            value=(f"`{self.search}`" if self.search else "`(none)`"),
            inline=True,
        )
        embed.add_field(
            name="Page",
            value=f"`{self.page + 1}/{total_pages}` • `{len(filtered)}` matches",
            inline=True,
        )

        embed.add_field(
            name="Selected Product",
            value=(
                f"**{pick_name(prod)}** (`{pick_id(prod)}`)\n"
                f"Price: `{pick_price(prod)}` • Stock: `{pick_stock(prod)}` • Status: `{pick_status(prod)}`"
                if prod else "—"
            ),
            inline=False,
        )

        embed.add_field(
            name="Selected Variant",
            value=(f"**{var['name']}** (`{var['id'] or 'no-id'}`)" if var else "—"),
            inline=False,
        )

        embed.set_footer(text="Tip: Use Search to find a product fast. Variant menu updates after selecting a product.")
        return embed

class ProductSelect(discord.ui.Select):
    def __init__(self, view_ref: OwnerManagerView):
        self.view_ref = view_ref
        super().__init__(
            placeholder="Select a product…",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="Loading…", value="none")]
        )

    def set_options(self, products_page: List[Dict[str, Any]], page: int, total_pages: int) -> None:
        options: List[discord.SelectOption] = []
        if not products_page:
            options = [discord.SelectOption(label="No products found", value="none")]
        else:
            for p in products_page:
                pid = pick_id(p)
                label = pick_name(p)[:100]
                desc = f"Price {pick_price(p)} • Stock {pick_stock(p)}"
                options.append(discord.SelectOption(label=label, value=pid, description=desc[:100]))

        self.options = options
        self.placeholder = f"Select a product… (page {page + 1}/{total_pages})"

    async def callback(self, interaction: discord.Interaction):
        pid = self.values[0]
        if pid == "none":
            return await interaction.response.send_message("No product selectable.", ephemeral=True)

        self.view_ref._selected_product_id = pid
        self.view_ref._selected_variant_key = None
        self.view_ref.variant_select.refresh_options()

        await interaction.response.edit_message(embed=self.view_ref.make_embed(), view=self.view_ref)

class VariantSelect(discord.ui.Select):
    def __init__(self, view_ref: OwnerManagerView):
        self.view_ref = view_ref
        super().__init__(
            placeholder="Select a variant (optional)…",
            min_values=0,
            max_values=1,
            options=[discord.SelectOption(label="Select a product first", value="none")]
        )

    def refresh_options(self) -> None:
        prod = self.view_ref.selected_product()
        if not prod:
            self.options = [discord.SelectOption(label="Select a product first", value="none")]
            self.placeholder = "Select a variant (optional)…"
            return

        variants = pick_variants(prod)
        if not variants:
            self.options = [discord.SelectOption(label="No variants for this product", value="none")]
            self.placeholder = "No variants"
            return

        options: List[discord.SelectOption] = []
        for v in variants[:25]:
            vkey = v["id"] if v["id"] else v["name"]
            label = v["name"][:100]
            desc = f"Price {v['price']} • Stock {v['stock']} • {('ID' if v['id'] else 'Name key')}"
            options.append(discord.SelectOption(label=label, value=str(vkey), description=desc[:100]))

        self.options = options
        self.placeholder = "Select a variant (optional)…"

    async def callback(self, interaction: discord.Interaction):
        if not self.values:
            self.view_ref._selected_variant_key = None
        else:
            v = self.values[0]
            self.view_ref._selected_variant_key = None if v == "none" else v

        await interaction.response.edit_message(embed=self.view_ref.make_embed(), view=self.view_ref)

class SearchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Search", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        await interaction.response.send_modal(SearchModal(view))

class PrevPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="◀ Page", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        view.page = max(0, view.page - 1)
        view.rebuild_product_select()
        await interaction.response.edit_message(embed=view.make_embed(), view=view)

class NextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Page ▶", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        view.page = view.page + 1
        view.rebuild_product_select()
        await interaction.response.edit_message(embed=view.make_embed(), view=view)

class RefreshButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Refresh", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        await interaction.response.defer(thinking=True, ephemeral=True)
        async with aiohttp.ClientSession() as session:
            try:
                view.products_cache = await bot.sellauth.list_products(session)
            except Exception as e:
                return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

        view.page = 0
        view.rebuild_product_select()
        await interaction.message.edit(embed=view.make_embed(), view=view)
        await interaction.followup.send("✅ Refreshed.", ephemeral=True)

class SetPriceButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Set Product Price", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        if not view.selected_product():
            return await interaction.response.send_message("Pick a product first.", ephemeral=True)
        await interaction.response.send_modal(SetProductPriceModal(view))

class ToggleEnabledButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Toggle Enabled", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        prod = view.selected_product()
        if not prod:
            return await interaction.response.send_message("Pick a product first.", ephemeral=True)

        cur_enabled = prod.get("enabled")
        if cur_enabled is None and prod.get("active") is not None:
            cur_enabled = bool(prod.get("active"))

        new_enabled = (not bool(cur_enabled)) if cur_enabled is not None else False

        await interaction.response.defer(thinking=True, ephemeral=True)
        async with aiohttp.ClientSession() as session:
            try:
                await bot.sellauth.update_product(session, pick_id(prod), {"enabled": new_enabled})
            except Exception as e:
                return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

            # refresh cache to reflect changes
            try:
                view.products_cache = await bot.sellauth.list_products(session)
            except Exception:
                pass

        view.rebuild_product_select()
        await interaction.message.edit(embed=view.make_embed(), view=view)
        await interaction.followup.send(f"✅ Set **{pick_name(prod)}** to `enabled={new_enabled}`.", ephemeral=True)

class SetVariantStockButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Set Variant Stock", style=discord.ButtonStyle.success)

    async def callback(self, interaction: discord.Interaction):
        view: OwnerManagerView = self.view  # type: ignore
        prod = view.selected_product()
        var = view.selected_variant()
        if not prod:
            return await interaction.response.send_message("Pick a product first.", ephemeral=True)
        if not var:
            return await interaction.response.send_message("Pick a variant first.", ephemeral=True)
        if not var["id"]:
            return await interaction.response.send_message("Variant has no ID in API payload.", ephemeral=True)

        await interaction.response.send_modal(SetVariantStockModal(view))

@bot.tree.command(name="manage_store", description="Owner: open dropdown manager for products/variants")
async def manage_store(interaction: discord.Interaction):
    if not is_owner(interaction):
        return await deny(interaction, "❌ Owner only.")

    await interaction.response.defer(thinking=True, ephemeral=True)
    async with aiohttp.ClientSession() as session:
        try:
            products = await bot.sellauth.list_products(session)
        except Exception as e:
            return await interaction.followup.send(f"Failed: `{e}`", ephemeral=True)

    if not products:
        return await interaction.followup.send("No products found.", ephemeral=True)

    view = OwnerManagerView(owner_id=interaction.user.id, products_cache=products)
    await interaction.followup.send(embed=view.make_embed(), view=view, ephemeral=True)


# ============================================================
# Poll loop (Option B)
# ============================================================
@tasks.loop(seconds=60)
async def poll_loop():
    async with aiohttp.ClientSession() as session:
        # PRODUCTS DIFF
        try:
            products = await bot.sellauth.list_products(session)
        except Exception as e:
            logging.warning("Products poll failed: %s", e)
            products = []

        if products:
            snap: Dict[str, Dict[str, Any]] = {}
            for p in products:
                pid = pick_id(p)
                if not pid:
                    continue
                snap[pid] = {
                    "name": pick_name(p),
                    "price": pick_price(p),
                    "stock": pick_stock(p),
                    "status": pick_status(p),
                    "variants": normalize_variant_map(p),
                }

            prev = bot.db.get_json("products_snapshot")

            if not prev:
                bot.db.set_json("products_snapshot", snap)
            else:
                new_products: List[str] = []
                removed_products: List[str] = []
                price_changes: List[str] = []
                stock_changes: List[str] = []
                status_changes: List[str] = []
                variant_new: List[str] = []
                variant_removed: List[str] = []
                variant_price_changes: List[str] = []
                variant_stock_changes: List[str] = []

                for pid, now in snap.items():
                    before = prev.get(pid)
                    if not before:
                        new_products.append(f"🆕 **{now['name']}** (`{pid}`)")
                        continue

                    if str(before.get("price")) != str(now.get("price")):
                        price_changes.append(
                            f"💲 **{now['name']}** — `{before.get('price','—')}` → `{now.get('price','—')}`"
                        )
                    if str(before.get("stock")) != str(now.get("stock")):
                        stock_changes.append(
                            f"📦 **{now['name']}** — `{before.get('stock','—')}` → `{now.get('stock','—')}`"
                        )
                    if str(before.get("status")) != str(now.get("status")):
                        status_changes.append(
                            f"🔧 **{now['name']}** — `{before.get('status','—')}` → `{now.get('status','—')}`"
                        )

                    before_vars = before.get("variants") or {}
                    now_vars = now.get("variants") or {}

                    for vkey, vnow in now_vars.items():
                        if vkey not in before_vars:
                            variant_new.append(
                                f"🧩 **{now['name']}** — new variant **{vnow.get('name','Variant')}**"
                            )
                            continue

                        vbefore = before_vars[vkey]
                        if str(vbefore.get("price")) != str(vnow.get("price")):
                            variant_price_changes.append(
                                f"💲 **{now['name']}** / **{vnow.get('name','Variant')}** — `{vbefore.get('price','—')}` → `{vnow.get('price','—')}`"
                            )
                        if str(vbefore.get("stock")) != str(vnow.get("stock")):
                            variant_stock_changes.append(
                                f"📦 **{now['name']}** / **{vnow.get('name','Variant')}** — `{vbefore.get('stock','—')}` → `{vnow.get('stock','—')}`"
                            )

                    for vkey, vbefore in before_vars.items():
                        if vkey not in now_vars:
                            variant_removed.append(
                                f"🧩 **{now['name']}** — removed variant **{vbefore.get('name','Variant')}**"
                            )

                for pid, before in prev.items():
                    if pid not in snap:
                        removed_products.append(f"🗑️ **{before.get('name','Unnamed')}** (`{pid}`)")

                if any([
                    new_products, removed_products,
                    price_changes, stock_changes, status_changes,
                    variant_new, variant_removed,
                    variant_price_changes, variant_stock_changes
                ]):
                    embed = discord.Embed(
                        title="🛒 Product Updates",
                        description="Detected changes since last poll.",
                        color=discord.Color.red(),
                    )

                    def add_section(title: str, lines: List[str], limit: int = 8):
                        if not lines:
                            return
                        shown = lines[:limit]
                        more = len(lines) - len(shown)
                        value = "\n".join(shown)
                        if more > 0:
                            value += f"\n… +{more} more"
                        embed.add_field(name=title, value=value, inline=False)

                    add_section("New Products", new_products)
                    add_section("Price Changes", price_changes)
                    add_section("Stock Changes", stock_changes)
                    add_section("Status Changes", status_changes)
                    add_section("New Variants", variant_new)
                    add_section("Variant Price Changes", variant_price_changes)
                    add_section("Variant Stock Changes", variant_stock_changes)
                    add_section("Removed Variants", variant_removed)
                    add_section("Removed Products", removed_products)

                    await post_embed("product_channel_id", embed)

                bot.db.set_json("products_snapshot", snap)

        # Tickets / Payments autopost can remain as you had it; omitted for brevity if your API differs.
        # If you want them back exactly, tell me and I’ll paste the full ticket/payment sections too.

@poll_loop.before_loop
async def before_poll():
    await bot.wait_until_ready()

if __name__ == "__main__":
    bot.run(cfg.discord_token)
