import datetime
import json
import random
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

from endstone import Player
from endstone.command import Command, CommandExecutor, CommandSender
from endstone.event import EventPriority, PlayerJoinEvent, PlayerQuitEvent, event_handler
from endstone.plugin import Plugin

try:
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover - optional dependency
    mysql_connector = None

COLOR_CODE_PATTERN = re.compile(r"&([0-9a-fk-orA-FK-ORg-uG-U])")


def _parse_version(version: str) -> Optional[tuple[int, ...]]:
    if not version:
        return None
    cleaned = version.strip()
    if cleaned.lower().startswith("v"):
        cleaned = cleaned[1:]
    parts = re.split(r"[.\-]", cleaned)
    numbers: list[int] = []
    for part in parts:
        if not part:
            continue
        match = re.match(r"(\d+)", part)
        if not match:
            break
        numbers.append(int(match.group(1)))
    return tuple(numbers) if numbers else None


def _is_newer_version(current: str, latest: str) -> bool:
    current_parsed = _parse_version(current)
    latest_parsed = _parse_version(latest)
    if current_parsed and latest_parsed:
        max_len = max(len(current_parsed), len(latest_parsed))
        current_parsed += (0,) * (max_len - len(current_parsed))
        latest_parsed += (0,) * (max_len - len(latest_parsed))
        return latest_parsed > current_parsed
    return latest.strip() != current.strip()


def colorize(text: str) -> str:
    if not text:
        return ""
    return COLOR_CODE_PATTERN.sub("\u00A7\\1", text)


def sanitize_player_name(player_name: str) -> str:
    if not player_name:
        return ""
    return re.sub(r"[\"';|&$`<>(){}\[\]]", "", player_name)


def format_player_for_command(player_name: str) -> str:
    safe_name = sanitize_player_name(player_name)
    if " " in safe_name:
        return f"\"{safe_name}\""
    return safe_name


def _get_nested(config: dict[str, Any], keys: list[str], default: Any) -> Any:
    current: Any = config
    for key in keys:
        if not isinstance(current, dict):
            return default
        if key not in current:
            return default
        current = current[key]
    return current


def _is_uuid_key(value: str) -> bool:
    if not value:
        return False
    cleaned = value.replace("-", "")
    if len(cleaned) != 32:
        return False
    return bool(re.fullmatch(r"[0-9a-fA-F]{32}", cleaned))


class UpdateCheckError(Exception):
    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


class EuphoriaMessagesCommandExecutor(CommandExecutor):
    def __init__(self, plugin: "EuphoriaMessagesPlugin") -> None:
        super().__init__()
        self._plugin = plugin

    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        if command.name != "euphoriamessages":
            return False

        if len(args) == 0:
            self._plugin.send_help(sender)
            return True

        subcommand = args[0].lower()

        if subcommand == "reload":
            if not sender.has_permission("euphoriamessages.reload"):
                sender.send_error_message("You don't have permission to reload the configuration.")
                return True
            self._plugin.reload_configuration()
            sender.send_message(colorize("&aEuphoriaMessages configuration reloaded successfully!"))
            return True

        if subcommand in {"broadcast", "bc"}:
            if not sender.has_permission("euphoriamessages.broadcast"):
                sender.send_error_message("You don't have permission to broadcast messages.")
                return True
            if len(args) < 2:
                sender.send_error_message("Usage: /euphoriamessages broadcast <message>")
                return True
            message = " ".join(args[1:])
            self._plugin.broadcast_message(message, sender)
            return True

        if subcommand == "preview":
            player = sender.as_player() if hasattr(sender, "as_player") else None
            if player is None:
                sender.send_error_message("This command can only be used by players.")
                return True
            if len(args) < 2:
                sender.send_error_message("Usage: /euphoriamessages preview <join|leave>")
                return True
            self._plugin.preview_message(player, args[1])
            return True

        if subcommand == "help":
            self._plugin.send_help(sender)
            return True

        if subcommand in {"info", "version"}:
            sender.send_message(colorize("&6=== EuphoriaMessages ==="))
            sender.send_message(colorize(f"&eVersion: &f{self._plugin.VERSION}"))
            sender.send_message(colorize("&eAuthor: &fRep Graphics"))
            sender.send_message(colorize("&eDescription: &fAdvanced join/leave messages"))
            return True

        sender.send_error_message("Unknown subcommand. Use /euphoriamessages help")
        return True


class WelcomeMessageListener:
    CLEANUP_INTERVAL_TICKS = 20 * 60 * 60  # 1 hour
    SAVE_INTERVAL_TICKS = 20 * 60 * 5  # 5 minutes
    ANNOUNCEMENT_TICK = 20  # 1 second

    JOIN_TIME_RETENTION_MS = 24 * 60 * 60 * 1000
    INACTIVE_RETENTION_MS = 30 * 24 * 60 * 60 * 1000
    DEFAULT_CONNECT_TIMEOUT = 5
    MAX_TABLE_PREFIX_LENGTH = 32

    def __init__(self, plugin: "EuphoriaMessagesPlugin") -> None:
        self._plugin = plugin
        self._random = random.Random()

        self.enabled = True
        self.default_join_message = ""
        self.default_leave_message = ""
        self.broadcast_join = True
        self.broadcast_leave = True
        self.role_join_messages: dict[str, str] = {}
        self.role_leave_messages: dict[str, str] = {}
        self.role_permission_cache: dict[str, str] = {}

        self.first_join_enabled = True
        self.first_join_message = ""
        self.player_count_enabled = True
        self.join_delay_ticks = 20

        self.random_messages_enabled = False
        self.random_join_messages: list[str] = []
        self.random_leave_messages: list[str] = []

        self.cooldown_enabled = False
        self.cooldown_seconds = 10
        self.last_join_time: dict[str, int] = {}

        self.time_based_greetings_enabled = False
        self.join_statistics_enabled = False
        self.welcome_back_timer_enabled = False
        self.player_milestones_enabled = False

        self.player_join_counts: dict[str, int] = {}
        self.player_last_seen: dict[str, int] = {}
        self.player_total_playtime: dict[str, int] = {}
        self.player_names: dict[str, str] = {}
        self.session_start: dict[str, int] = {}
        self.known_players: set[str] = set()

        self.milestone_joins: list[int] = []
        self.milestone_join_rewards: list[str] = []
        self.milestone_playtime_hours: list[int] = []
        self.milestone_playtime_rewards: list[str] = []
        self.awarded_join_milestones: dict[str, set[int]] = {}
        self.awarded_playtime_milestones: dict[str, set[int]] = {}

        self.auto_announcements_enabled = False
        self.announcements: list[str] = []
        self.announcement_interval_seconds = 300
        self.current_announcement_index = 0
        self.next_announcement_at: Optional[float] = None

        self.log_joins = False
        self.log_leaves = False
        self.log_first_joins = True
        self.log_feature_status = False

        self._pending_player_data_save = False
        self._dirty_players: set[str] = set()

        self._legacy_join_counts: dict[str, int] = {}
        self._legacy_last_seen: dict[str, int] = {}
        self._legacy_playtime: dict[str, int] = {}
        self._legacy_awarded_join_milestones: dict[str, set[int]] = {}
        self._legacy_awarded_playtime_milestones: dict[str, set[int]] = {}
        self._legacy_known_players: set[str] = set()

        self._data_file = Path(self._plugin.data_folder) / "playerdata.json"
        self.storage = "json"
        self.mysql_config: dict[str, Any] = {}
        self.mysql_settings: dict[str, Any] = {}
        self.mysql_table_prefix = "euphoriamessages_"
        self.mysql_connection = None
        self.persistence_enabled = True

        self.load_config(initial=True)
        self.load_player_data()

        self._plugin.server.scheduler.run_task(
            self._plugin,
            self.cleanup_old_join_times,
            delay=self.CLEANUP_INTERVAL_TICKS,
            period=self.CLEANUP_INTERVAL_TICKS,
        )
        self._plugin.server.scheduler.run_task(
            self._plugin,
            self._flush_pending_player_data,
            delay=self.SAVE_INTERVAL_TICKS,
            period=self.SAVE_INTERVAL_TICKS,
        )
        self._plugin.server.scheduler.run_task(
            self._plugin,
            self._auto_announcement_tick,
            delay=self.ANNOUNCEMENT_TICK,
            period=self.ANNOUNCEMENT_TICK,
        )

    @staticmethod
    def _as_int(value, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    def _read_storage_config(self, config: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        storage_config = config.get("player-data", {})
        if not isinstance(storage_config, dict):
            storage_config = {}

        storage = str(storage_config.get("storage", "json")).lower()
        mysql_config = storage_config.get("mysql", {})
        if not isinstance(mysql_config, dict):
            mysql_config = {}

        if mysql_config.get("enabled", False):
            storage = "mysql"

        if storage in {"yaml", "file"}:
            storage = "json"

        if storage not in {"json", "mysql"}:
            self._plugin.logger.warning(
                f"Unknown player-data storage '{storage}', falling back to json."
            )
            storage = "json"

        return storage, mysql_config

    def _initialize_storage(self) -> None:
        self.persistence_enabled = True
        if self.storage == "mysql":
            if not self.initialize_mysql_backend():
                self.storage = "json"
                self.persistence_enabled = True
                self._plugin.logger.warning(
                    "Falling back to JSON player data backend due to MySQL initialization failure."
                )
        else:
            self.close_mysql()

    def _reconfigure_storage(self) -> None:
        payload = self._build_player_data_payload()
        self.close_mysql()
        self._initialize_storage()

        if self.storage == "mysql" and self.persistence_enabled:
            self._save_player_data_to_mysql(None)
            if (
                self._legacy_join_counts
                or self._legacy_last_seen
                or self._legacy_playtime
                or self._legacy_awarded_join_milestones
                or self._legacy_awarded_playtime_milestones
                or self._legacy_known_players
            ):
                self._save_player_data_to_json(payload)
        else:
            self._save_player_data_to_json(payload)

        self._dirty_players.clear()
        self._pending_player_data_save = False

    def initialize_mysql_backend(self) -> bool:
        if mysql_connector is None:
            self._plugin.logger.error(
                "MySQL storage selected but mysql-connector-python is not installed."
            )
            return False

        if not isinstance(self.mysql_config, dict):
            self._plugin.logger.error("Invalid player-data.mysql configuration block.")
            return False

        host = str(self.mysql_config.get("host", "127.0.0.1"))
        port = self._as_int(self.mysql_config.get("port", 3306), 3306)
        if port < 1 or port > 65535:
            port = 3306

        database = str(self.mysql_config.get("database", "")).strip()
        user = str(self.mysql_config.get("user", "")).strip()
        password = str(self.mysql_config.get("password", ""))
        connect_timeout = self._as_int(
            self.mysql_config.get("connect-timeout", self.DEFAULT_CONNECT_TIMEOUT),
            self.DEFAULT_CONNECT_TIMEOUT,
        )
        if connect_timeout < 1:
            connect_timeout = self.DEFAULT_CONNECT_TIMEOUT

        table_prefix = str(self.mysql_config.get("table-prefix", "euphoriamessages_"))
        table_prefix = re.sub(r"[^a-zA-Z0-9_]", "", table_prefix)
        if len(table_prefix) > self.MAX_TABLE_PREFIX_LENGTH:
            table_prefix = table_prefix[: self.MAX_TABLE_PREFIX_LENGTH]
        if not table_prefix:
            table_prefix = "euphoriamessages_"
        self.mysql_table_prefix = table_prefix

        if not database or not user:
            self._plugin.logger.error(
                "MySQL storage requires player-data.mysql.database and player-data.mysql.user."
            )
            return False

        self.mysql_settings = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "connection_timeout": connect_timeout,
            "autocommit": False,
        }

        try:
            conn = self.get_mysql_connection()
            self.ensure_mysql_tables(conn)
            self._plugin.logger.info(
                f"MySQL player data backend initialized ({host}:{port}/{database})"
            )
            return True
        except Exception as exc:
            self._plugin.logger.error(f"Failed to initialize MySQL backend: {exc}")
            self.close_mysql()
            return False

    def get_mysql_connection(self):
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")

        if self.mysql_connection is not None:
            try:
                if self.mysql_connection.is_connected():
                    self.mysql_connection.ping(reconnect=True, attempts=1, delay=0)
                    return self.mysql_connection
            except Exception:
                try:
                    self.mysql_connection.close()
                except Exception:
                    pass
                self.mysql_connection = None

        self.mysql_connection = mysql_connector.connect(**self.mysql_settings)
        return self.mysql_connection

    def ensure_mysql_tables(self, conn) -> None:
        data_table = f"{self.mysql_table_prefix}player_data"
        milestones_table = f"{self.mysql_table_prefix}player_milestones"

        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{data_table}` (
                    `player_key` VARCHAR(36) NOT NULL,
                    `player_name` VARCHAR(64) NOT NULL DEFAULT '',
                    `join_count` BIGINT NOT NULL DEFAULT 0,
                    `last_seen` BIGINT NOT NULL DEFAULT 0,
                    `playtime_ms` BIGINT NOT NULL DEFAULT 0,
                    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`player_key`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{milestones_table}` (
                    `player_key` VARCHAR(36) NOT NULL,
                    `milestone_type` VARCHAR(16) NOT NULL,
                    `milestone` INT NOT NULL,
                    PRIMARY KEY (`player_key`, `milestone_type`, `milestone`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            conn.commit()
        finally:
            cursor.close()

    def close_mysql(self) -> None:
        if self.mysql_connection is not None:
            try:
                self.mysql_connection.close()
            except Exception:
                pass
            self.mysql_connection = None

    def _build_player_data_payload(self) -> dict[str, Any]:
        join_counts = dict(self._legacy_join_counts)
        join_counts.update(self.player_join_counts)

        last_seen = dict(self._legacy_last_seen)
        last_seen.update(self.player_last_seen)

        playtime_ms = dict(self._legacy_playtime)
        playtime_ms.update(self.player_total_playtime)

        awarded_join = {key: sorted(values) for key, values in self._legacy_awarded_join_milestones.items()}
        awarded_join.update({key: sorted(values) for key, values in self.awarded_join_milestones.items()})

        awarded_playtime = {
            key: sorted(values) for key, values in self._legacy_awarded_playtime_milestones.items()
        }
        awarded_playtime.update(
            {key: sorted(values) for key, values in self.awarded_playtime_milestones.items()}
        )

        known_players = sorted(set(self.known_players).union(self._legacy_known_players))

        payload = {
            "known_players": known_players,
            "join_counts": join_counts,
            "last_seen": last_seen,
            "playtime_ms": playtime_ms,
            "awarded_join_milestones": awarded_join,
            "awarded_playtime_milestones": awarded_playtime,
        }

        if self.player_names:
            payload["player_names"] = dict(self.player_names)

        return payload

    def _apply_player_data(self, data: dict[str, Any]) -> None:
        self.player_join_counts.clear()
        self.player_last_seen.clear()
        self.player_total_playtime.clear()
        self.player_names.clear()
        self.known_players.clear()
        self.awarded_join_milestones.clear()
        self.awarded_playtime_milestones.clear()
        self._legacy_join_counts.clear()
        self._legacy_last_seen.clear()
        self._legacy_playtime.clear()
        self._legacy_awarded_join_milestones.clear()
        self._legacy_awarded_playtime_milestones.clear()
        self._legacy_known_players.clear()

        join_counts = data.get("join_counts", {}) or {}
        for key, value in join_counts.items():
            key_str = str(key).lower()
            if _is_uuid_key(key_str):
                self.player_join_counts[key_str] = self._as_int(value, 0)
            else:
                self._legacy_join_counts[key_str] = self._as_int(value, 0)

        last_seen = data.get("last_seen", {}) or {}
        for key, value in last_seen.items():
            key_str = str(key).lower()
            if _is_uuid_key(key_str):
                self.player_last_seen[key_str] = self._as_int(value, 0)
            else:
                self._legacy_last_seen[key_str] = self._as_int(value, 0)

        playtime = data.get("playtime_ms", {}) or {}
        for key, value in playtime.items():
            key_str = str(key).lower()
            if _is_uuid_key(key_str):
                self.player_total_playtime[key_str] = self._as_int(value, 0)
            else:
                self._legacy_playtime[key_str] = self._as_int(value, 0)

        known_players = data.get("known_players", [])
        if isinstance(known_players, list):
            for name in known_players:
                key_str = str(name).lower()
                if _is_uuid_key(key_str):
                    self.known_players.add(key_str)
                else:
                    self._legacy_known_players.add(key_str)

        player_names = data.get("player_names", {}) or {}
        if isinstance(player_names, dict):
            for key, value in player_names.items():
                key_str = str(key).lower()
                if _is_uuid_key(key_str):
                    self.player_names[key_str] = str(value)

        join_milestones = data.get("awarded_join_milestones", {}) or {}
        for key, values in join_milestones.items():
            key_str = str(key).lower()
            milestones = {self._as_int(v, 0) for v in values or [] if self._as_int(v, 0) > 0}
            if not milestones:
                continue
            if _is_uuid_key(key_str):
                self.awarded_join_milestones[key_str] = milestones
            else:
                self._legacy_awarded_join_milestones[key_str] = milestones

        playtime_milestones = data.get("awarded_playtime_milestones", {}) or {}
        for key, values in playtime_milestones.items():
            key_str = str(key).lower()
            milestones = {self._as_int(v, 0) for v in values or [] if self._as_int(v, 0) > 0}
            if not milestones:
                continue
            if _is_uuid_key(key_str):
                self.awarded_playtime_milestones[key_str] = milestones
            else:
                self._legacy_awarded_playtime_milestones[key_str] = milestones

    def _load_player_data_from_json(self) -> None:
        self._data_file.parent.mkdir(parents=True, exist_ok=True)
        if not self._data_file.exists():
            self._data_file.write_text("{}", encoding="utf-8")

        try:
            data = json.loads(self._data_file.read_text(encoding="utf-8"))
        except Exception as exc:
            self._plugin.logger.error(f"Failed to read playerdata.json: {exc}")
            data = {}

        if data is None:
            data = {}

        self._apply_player_data(data)

    def _load_legacy_data_from_json(self) -> None:
        self._legacy_join_counts.clear()
        self._legacy_last_seen.clear()
        self._legacy_playtime.clear()
        self._legacy_awarded_join_milestones.clear()
        self._legacy_awarded_playtime_milestones.clear()
        self._legacy_known_players.clear()

        if not self._data_file.exists():
            return

        try:
            data = json.loads(self._data_file.read_text(encoding="utf-8"))
        except Exception:
            return

        if not isinstance(data, dict):
            return

        join_counts = data.get("join_counts", {}) or {}
        for key, value in join_counts.items():
            key_str = str(key).lower()
            if not _is_uuid_key(key_str):
                self._legacy_join_counts[key_str] = self._as_int(value, 0)

        last_seen = data.get("last_seen", {}) or {}
        for key, value in last_seen.items():
            key_str = str(key).lower()
            if not _is_uuid_key(key_str):
                self._legacy_last_seen[key_str] = self._as_int(value, 0)

        playtime = data.get("playtime_ms", {}) or {}
        for key, value in playtime.items():
            key_str = str(key).lower()
            if not _is_uuid_key(key_str):
                self._legacy_playtime[key_str] = self._as_int(value, 0)

        known_players = data.get("known_players", [])
        if isinstance(known_players, list):
            for name in known_players:
                key_str = str(name).lower()
                if not _is_uuid_key(key_str):
                    self._legacy_known_players.add(key_str)

        join_milestones = data.get("awarded_join_milestones", {}) or {}
        for key, values in join_milestones.items():
            key_str = str(key).lower()
            if _is_uuid_key(key_str):
                continue
            milestones = {self._as_int(v, 0) for v in values or [] if self._as_int(v, 0) > 0}
            if milestones:
                self._legacy_awarded_join_milestones[key_str] = milestones

        playtime_milestones = data.get("awarded_playtime_milestones", {}) or {}
        for key, values in playtime_milestones.items():
            key_str = str(key).lower()
            if _is_uuid_key(key_str):
                continue
            milestones = {self._as_int(v, 0) for v in values or [] if self._as_int(v, 0) > 0}
            if milestones:
                self._legacy_awarded_playtime_milestones[key_str] = milestones

    def _delete_legacy_mysql_rows(self, legacy_key: str) -> None:
        if self.storage != "mysql" or not self.persistence_enabled:
            return
        if not legacy_key:
            return
        try:
            conn = self.get_mysql_connection()
            data_table = f"{self.mysql_table_prefix}player_data"
            milestones_table = f"{self.mysql_table_prefix}player_milestones"
            cursor = conn.cursor()
            try:
                cursor.execute(f"DELETE FROM `{data_table}` WHERE player_key = %s", (legacy_key,))
                cursor.execute(
                    f"DELETE FROM `{milestones_table}` WHERE player_key = %s", (legacy_key,)
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        except Exception as exc:
            self._plugin.logger.error(f"Failed to delete legacy MySQL data: {exc}")

    def _migrate_legacy_name_data(self, player: Player) -> None:
        name_key = player.name.lower()
        uuid_key = str(player.unique_id).lower()
        if _is_uuid_key(uuid_key) is False:
            return

        migrated = False

        if uuid_key not in self.player_join_counts and name_key in self._legacy_join_counts:
            self.player_join_counts[uuid_key] = self._legacy_join_counts.pop(name_key)
            migrated = True
        if uuid_key not in self.player_last_seen and name_key in self._legacy_last_seen:
            self.player_last_seen[uuid_key] = self._legacy_last_seen.pop(name_key)
            migrated = True
        if uuid_key not in self.player_total_playtime and name_key in self._legacy_playtime:
            self.player_total_playtime[uuid_key] = self._legacy_playtime.pop(name_key)
            migrated = True
        if uuid_key not in self.awarded_join_milestones and name_key in self._legacy_awarded_join_milestones:
            self.awarded_join_milestones[uuid_key] = self._legacy_awarded_join_milestones.pop(
                name_key
            )
            migrated = True
        if (
            uuid_key not in self.awarded_playtime_milestones
            and name_key in self._legacy_awarded_playtime_milestones
        ):
            self.awarded_playtime_milestones[
                uuid_key
            ] = self._legacy_awarded_playtime_milestones.pop(name_key)
            migrated = True
        if name_key in self._legacy_known_players:
            self._legacy_known_players.discard(name_key)
            self.known_players.add(uuid_key)
            migrated = True

        if migrated:
            self.player_names[uuid_key] = player.name
            self._dirty_players.add(uuid_key)
            self._pending_player_data_save = True
            if self.storage == "mysql" and self.persistence_enabled:
                self._delete_legacy_mysql_rows(name_key)

    def _write_player_data_payload(self, data: dict[str, Any]) -> None:
        if self.storage == "mysql":
            self._save_player_data_to_mysql(None)
        else:
            self._save_player_data_to_json(data)

    def _save_player_data_to_json(self, data: dict[str, Any]) -> None:
        try:
            self._data_file.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        except Exception as exc:
            self._plugin.logger.error(f"Failed to save playerdata.json: {exc}")

    def _load_player_data_from_mysql(self) -> int:
        self.player_join_counts.clear()
        self.player_last_seen.clear()
        self.player_total_playtime.clear()
        self.known_players.clear()
        self.awarded_join_milestones.clear()
        self.awarded_playtime_milestones.clear()
        self.player_names.clear()

        loaded_rows = 0
        try:
            conn = self.get_mysql_connection()
            data_table = f"{self.mysql_table_prefix}player_data"
            milestones_table = f"{self.mysql_table_prefix}player_milestones"

            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SELECT player_key, player_name, join_count, last_seen, playtime_ms "
                    f"FROM `{data_table}`"
                )
                for row in cursor.fetchall():
                    player_key = str(row[0]).lower()
                    player_name = str(row[1]) if row[1] is not None else ""
                    join_count = self._as_int(row[2], 0)
                    last_seen = self._as_int(row[3], 0)
                    playtime_ms = self._as_int(row[4], 0)

                    if _is_uuid_key(player_key):
                        self.known_players.add(player_key)
                        self.player_join_counts[player_key] = join_count
                        if last_seen:
                            self.player_last_seen[player_key] = last_seen
                        if playtime_ms:
                            self.player_total_playtime[player_key] = playtime_ms
                        if player_name:
                            self.player_names[player_key] = player_name
                        loaded_rows += 1
                    else:
                        self._legacy_known_players.add(player_key)
                        self._legacy_join_counts[player_key] = join_count
                        if last_seen:
                            self._legacy_last_seen[player_key] = last_seen
                        if playtime_ms:
                            self._legacy_playtime[player_key] = playtime_ms

                cursor.execute(
                    f"SELECT player_key, milestone_type, milestone FROM `{milestones_table}`"
                )
                for row in cursor.fetchall():
                    player_key = str(row[0]).lower()
                    milestone_type = str(row[1]).lower()
                    milestone_value = self._as_int(row[2], 0)
                    if milestone_value <= 0:
                        continue
                    if _is_uuid_key(player_key):
                        if milestone_type == "join":
                            self.awarded_join_milestones.setdefault(player_key, set()).add(
                                milestone_value
                            )
                        elif milestone_type == "playtime":
                            self.awarded_playtime_milestones.setdefault(player_key, set()).add(
                                milestone_value
                            )
                    else:
                        if milestone_type == "join":
                            self._legacy_awarded_join_milestones.setdefault(player_key, set()).add(
                                milestone_value
                            )
                        elif milestone_type == "playtime":
                            self._legacy_awarded_playtime_milestones.setdefault(
                                player_key, set()
                            ).add(milestone_value)
            finally:
                cursor.close()
        except Exception as exc:
            self._plugin.logger.error(f"Error loading MySQL player data: {exc}")
        return loaded_rows

    def _save_player_data_to_mysql(self, player_keys: Optional[set[str]]) -> None:
        try:
            conn = self.get_mysql_connection()
            data_table = f"{self.mysql_table_prefix}player_data"
            milestones_table = f"{self.mysql_table_prefix}player_milestones"

            cursor = conn.cursor()
            try:
                upsert_sql = (
                    f"INSERT INTO `{data_table}` "
                    "(player_key, player_name, join_count, last_seen, playtime_ms) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "player_name = VALUES(player_name), "
                    "join_count = VALUES(join_count), "
                    "last_seen = VALUES(last_seen), "
                    "playtime_ms = VALUES(playtime_ms)"
                )

                delete_milestones_sql = f"DELETE FROM `{milestones_table}` WHERE player_key = %s"
                insert_milestone_sql = (
                    f"INSERT INTO `{milestones_table}` "
                    "(player_key, milestone_type, milestone) VALUES (%s, %s, %s)"
                )

                keys_to_save = (
                    {str(key).lower() for key in player_keys}
                    if player_keys is not None
                    else set(self.known_players)
                )
                keys_to_save.update(self.player_join_counts.keys())
                keys_to_save.update(self.player_last_seen.keys())
                keys_to_save.update(self.player_total_playtime.keys())
                keys_to_save.update(self.awarded_join_milestones.keys())
                keys_to_save.update(self.awarded_playtime_milestones.keys())

                for player_key in sorted(keys_to_save):
                    key_str = str(player_key).lower()
                    if not _is_uuid_key(key_str):
                        continue
                    join_count_value = self.player_join_counts.get(key_str, 0)
                    last_seen_value = self.player_last_seen.get(key_str, 0)
                    playtime_value = self.player_total_playtime.get(key_str, 0)
                    join_milestone_values = self.awarded_join_milestones.get(key_str, set())
                    playtime_milestone_values = self.awarded_playtime_milestones.get(
                        key_str, set()
                    )
                    player_name_value = self.player_names.get(key_str, "")
                    cursor.execute(
                        upsert_sql,
                        (
                            key_str,
                            player_name_value,
                            self._as_int(join_count_value, 0),
                            self._as_int(last_seen_value, 0),
                            self._as_int(playtime_value, 0),
                        ),
                    )

                    cursor.execute(delete_milestones_sql, (key_str,))
                    milestone_rows: list[tuple[str, str, int]] = []
                    for milestone in join_milestone_values or []:
                        milestone_rows.append((key_str, "join", self._as_int(milestone, 0)))
                    for milestone in playtime_milestone_values or []:
                        milestone_rows.append((key_str, "playtime", self._as_int(milestone, 0)))
                    milestone_rows = [
                        row for row in milestone_rows if row[2] > 0
                    ]
                    if milestone_rows:
                        cursor.executemany(insert_milestone_sql, milestone_rows)

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        except Exception as exc:
            self._plugin.logger.error(f"Error saving MySQL player data: {exc}")

    def load_player_data(self) -> None:
        if self.storage == "mysql" and self.persistence_enabled:
            loaded_rows = self._load_player_data_from_mysql()
            if loaded_rows == 0:
                self._load_player_data_from_json()
                has_data = bool(
                    self.known_players
                    or self.player_join_counts
                    or self.player_last_seen
                    or self.player_total_playtime
                    or self.awarded_join_milestones
                    or self.awarded_playtime_milestones
                    or self._legacy_known_players
                    or self._legacy_join_counts
                    or self._legacy_last_seen
                    or self._legacy_playtime
                    or self._legacy_awarded_join_milestones
                    or self._legacy_awarded_playtime_milestones
                )
                if has_data:
                    self._save_player_data_to_mysql(None)
            else:
                self._load_legacy_data_from_json()
        else:
            self._load_player_data_from_json()

    def _write_player_data(self) -> None:
        if self.storage == "mysql" and self.persistence_enabled:
            if self._dirty_players:
                self._save_player_data_to_mysql(set(self._dirty_players))
                self._dirty_players.clear()

            if (
                self._legacy_join_counts
                or self._legacy_last_seen
                or self._legacy_playtime
                or self._legacy_awarded_join_milestones
                or self._legacy_awarded_playtime_milestones
                or self._legacy_known_players
            ):
                data = self._build_player_data_payload()
                self._save_player_data_to_json(data)
            return

        data = self._build_player_data_payload()
        self._save_player_data_to_json(data)

    def _flush_pending_player_data(self) -> None:
        if self._pending_player_data_save:
            self._write_player_data()
            self._pending_player_data_save = False

    def save_pending_data(self) -> None:
        if self._pending_player_data_save:
            self._write_player_data()
            self._pending_player_data_save = False
        self.close_mysql()

    def load_config(self, initial: bool = False) -> None:
        config = self._plugin.config
        new_storage, new_mysql_config = self._read_storage_config(config)
        if initial:
            self.storage = new_storage
            self.mysql_config = new_mysql_config
            self._initialize_storage()
        elif self.storage != new_storage or self.mysql_config != new_mysql_config:
            self.storage = new_storage
            self.mysql_config = new_mysql_config
            self._reconfigure_storage()

        self.enabled = bool(_get_nested(config, ["welcome-messages", "enabled"], True))
        self.broadcast_join = bool(_get_nested(config, ["welcome-messages", "broadcast-join"], True))
        self.broadcast_leave = bool(_get_nested(config, ["welcome-messages", "broadcast-leave"], True))

        self.default_join_message = _get_nested(
            config,
            ["welcome-messages", "default-join-message"],
            "&a{player} has joined the server",
        )
        self.default_leave_message = _get_nested(
            config,
            ["welcome-messages", "default-leave-message"],
            "&c{player} has left the server",
        )

        self.first_join_enabled = bool(
            _get_nested(config, ["welcome-messages", "first-join", "enabled"], True)
        )
        self.first_join_message = _get_nested(
            config,
            ["welcome-messages", "first-join", "message"],
            "&e&l* &6Welcome &e{player} &6to the server for the first time! &e&l*",
        )

        self.player_count_enabled = bool(
            _get_nested(config, ["welcome-messages", "player-count", "enabled"], True)
        )
        self.join_delay_ticks = int(_get_nested(config, ["welcome-messages", "join-delay-ticks"], 20))

        self.random_messages_enabled = bool(
            _get_nested(config, ["welcome-messages", "random-messages", "enabled"], False)
        )
        self.random_join_messages = list(
            _get_nested(config, ["welcome-messages", "random-messages", "join-messages"], []) or []
        )
        self.random_leave_messages = list(
            _get_nested(config, ["welcome-messages", "random-messages", "leave-messages"], []) or []
        )

        if self.random_messages_enabled and not self.random_join_messages and not self.random_leave_messages:
            self.random_messages_enabled = False
            self._plugin.logger.warning(
                "Random messages enabled but no messages configured. Feature disabled."
            )

        self.cooldown_enabled = bool(
            _get_nested(config, ["welcome-messages", "rejoin-cooldown", "enabled"], False)
        )
        self.cooldown_seconds = int(
            _get_nested(config, ["welcome-messages", "rejoin-cooldown", "seconds"], 10)
        )

        self.time_based_greetings_enabled = bool(
            _get_nested(config, ["advanced-features", "time-based-greetings", "enabled"], False)
        )
        self.join_statistics_enabled = bool(
            _get_nested(config, ["advanced-features", "join-statistics", "enabled"], False)
        )
        self.welcome_back_timer_enabled = bool(
            _get_nested(config, ["advanced-features", "welcome-back-timer", "enabled"], False)
        )
        self.player_milestones_enabled = bool(
            _get_nested(config, ["advanced-features", "player-milestones", "enabled"], False)
        )

        self.milestone_joins = list(
            _get_nested(config, ["advanced-features", "player-milestones", "join-milestones"], []) or []
        )
        self.milestone_join_rewards = list(
            _get_nested(config, ["advanced-features", "player-milestones", "join-rewards"], []) or []
        )
        self.milestone_playtime_hours = list(
            _get_nested(
                config, ["advanced-features", "player-milestones", "playtime-milestones"], []
            )
            or []
        )
        self.milestone_playtime_rewards = list(
            _get_nested(
                config, ["advanced-features", "player-milestones", "playtime-rewards"], []
            )
            or []
        )

        self.auto_announcements_enabled = bool(
            _get_nested(config, ["advanced-features", "auto-announcements", "enabled"], False)
        )
        interval_minutes = int(
            _get_nested(config, ["advanced-features", "auto-announcements", "interval-minutes"], 5)
        )
        self.announcement_interval_seconds = max(interval_minutes, 1) * 60
        self.announcements = list(
            _get_nested(config, ["advanced-features", "auto-announcements", "messages"], []) or []
        )
        if self.auto_announcements_enabled and not self.announcements:
            self.auto_announcements_enabled = False

        self.log_joins = bool(_get_nested(config, ["welcome-messages", "logging", "log-joins"], False))
        self.log_leaves = bool(_get_nested(config, ["welcome-messages", "logging", "log-leaves"], False))
        self.log_first_joins = bool(
            _get_nested(config, ["welcome-messages", "logging", "log-first-joins"], True)
        )
        self.log_feature_status = bool(
            _get_nested(config, ["welcome-messages", "logging", "log-feature-status"], False)
        )

        if self.log_feature_status:
            self._plugin.logger.info("Feature Status:")
            self._plugin.logger.info(f"  - Join messages: {self.broadcast_join}")
            self._plugin.logger.info(f"  - Leave messages: {self.broadcast_leave}")
            self._plugin.logger.info(f"  - First join: {self.first_join_enabled}")
            self._plugin.logger.info(f"  - Player count: {self.player_count_enabled}")
            self._plugin.logger.info(f"  - Random messages: {self.random_messages_enabled}")
            self._plugin.logger.info(f"  - Cooldown: {self.cooldown_enabled}")

        self.role_join_messages.clear()
        self.role_leave_messages.clear()
        self.role_permission_cache.clear()

        roles = _get_nested(config, ["welcome-messages", "roles"], {}) or {}
        if isinstance(roles, dict):
            for role_name, role_data in roles.items():
                if not isinstance(role_data, dict):
                    continue
                join_message = role_data.get("join-message")
                if join_message:
                    self.role_join_messages[str(role_name)] = str(join_message)
                leave_message = role_data.get("leave-message")
                if leave_message:
                    self.role_leave_messages[str(role_name)] = str(leave_message)

                role_name_lower = str(role_name).lower()
                if role_name_lower not in {"op", "admin"}:
                    self.role_permission_cache[str(role_name)] = (
                        f"euphoriamessages.role.{role_name_lower}"
                    )

        self.reset_announcement_timer()

    def reset_announcement_timer(self) -> None:
        if self.auto_announcements_enabled and self.announcements:
            self.next_announcement_at = time.time() + self.announcement_interval_seconds
        else:
            self.next_announcement_at = None

    @event_handler(priority=EventPriority.HIGHEST)
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        player = event.player
        player_key = str(player.unique_id).lower()
        self._migrate_legacy_name_data(player)
        if self.player_names.get(player_key) != player.name:
            self.player_names[player_key] = player.name
            self._dirty_players.add(player_key)
            self._pending_player_data_save = True
        is_first_join = player_key not in self.known_players

        silent_join = player.has_permission("euphoriamessages.silent")

        if self.join_statistics_enabled:
            current_count = self.player_join_counts.get(player_key, 0)
            self.player_join_counts[player_key] = current_count + 1
            self._pending_player_data_save = True
            self._dirty_players.add(player_key)

        previous_last_seen = self.player_last_seen.get(player_key)
        self.session_start[player_key] = int(time.time() * 1000)

        on_cooldown = False
        if self.cooldown_enabled and not is_first_join:
            last_time = self.last_join_time.get(player_key)
            if last_time is not None:
                time_since = (int(time.time() * 1000) - last_time) / 1000.0
                if time_since < self.cooldown_seconds:
                    on_cooldown = True

        self.last_join_time[player_key] = int(time.time() * 1000)

        if is_first_join:
            self.known_players.add(player_key)
            self._pending_player_data_save = True
            self._dirty_players.add(player_key)
            if self.log_first_joins:
                self._plugin.logger.info(f"First join: {player.name}")
        elif self.log_joins:
            self._plugin.logger.info(f"Player joined: {player.name}")

        event.join_message = ""

        if not self.enabled:
            return

        if self.player_milestones_enabled:
            self.check_milestones(player)

        should_broadcast = (is_first_join and self.first_join_enabled) or (
            self.broadcast_join and not on_cooldown
        )
        if should_broadcast and not silent_join:
            self._plugin.server.scheduler.run_task(
                self._plugin,
                lambda: self._send_join_message(player, is_first_join, previous_last_seen),
                delay=self.join_delay_ticks,
            )

    @event_handler(priority=EventPriority.HIGHEST)
    def on_player_quit(self, event: PlayerQuitEvent) -> None:
        player = event.player
        player_key = str(player.unique_id).lower()
        if self.player_names.get(player_key) != player.name:
            self.player_names[player_key] = player.name
            self._dirty_players.add(player_key)
            self._pending_player_data_save = True

        session_start = self.session_start.pop(player_key, None)
        if session_start is not None:
            session_time = int(time.time() * 1000) - session_start
            total_playtime = self.player_total_playtime.get(player_key, 0)
            self.player_total_playtime[player_key] = total_playtime + max(session_time, 0)
            self._pending_player_data_save = True
            self._dirty_players.add(player_key)

        self.player_last_seen[player_key] = int(time.time() * 1000)
        self._pending_player_data_save = True
        self._dirty_players.add(player_key)

        silent_leave = player.has_permission("euphoriamessages.silent")

        if self.log_leaves:
            self._plugin.logger.info(f"Player left: {player.name}")

        if not self.enabled or not self.broadcast_leave or silent_leave:
            event.quit_message = ""
            return

        if self.random_messages_enabled and self.random_leave_messages:
            message = self._random.choice(self.random_leave_messages)
        else:
            message = self.get_leave_message_for_player(player)

        event.quit_message = self.format_message(message, player, None)

    def _send_join_message(
        self, player: Player, is_first_join: bool, previous_last_seen: Optional[int]
    ) -> None:
        if player not in self._plugin.server.online_players:
            return

        if is_first_join and self.first_join_enabled:
            message = self.first_join_message
        elif self.random_messages_enabled and self.random_join_messages:
            message = self._random.choice(self.random_join_messages)
        else:
            message = self.get_join_message_for_player(player)

        formatted = self.format_message(message, player, previous_last_seen)
        if formatted:
            self._plugin.server.broadcast_message(formatted)

    def get_join_message_for_player(self, player: Player) -> str:
        for role_name, role_message in self.role_join_messages.items():
            if self.has_role(player, role_name):
                return role_message
        return self.default_join_message

    def get_leave_message_for_player(self, player: Player) -> str:
        for role_name, role_message in self.role_leave_messages.items():
            if self.has_role(player, role_name):
                return role_message
        return self.default_leave_message

    def has_role(self, player: Player, role_name: str) -> bool:
        role_lower = role_name.lower()
        if role_lower in {"op", "admin"}:
            return bool(getattr(player, "is_op", False))

        permission = self.role_permission_cache.get(role_name)
        if permission is None:
            permission = f"euphoriamessages.role.{role_lower}"

        return player.has_permission(permission)

    def format_message(
        self,
        message: str,
        player: Player,
        last_seen_override: Optional[int],
    ) -> str:
        if not message:
            return ""

        result = message

        player_name = player.name
        display_name = getattr(player, "name_tag", None) or player_name
        result = (
            result.replace("{player}", player_name)
            .replace("{name}", player_name)
            .replace("{displayname}", display_name)
        )

        result = result.replace("{ping}", str(getattr(player, "ping", "0")))

        level = getattr(player, "level", None)
        world_name = getattr(level, "name", "unknown") if level is not None else "unknown"
        result = result.replace("{world}", str(world_name))

        game_mode = getattr(player, "game_mode", None)
        if game_mode is None:
            game_mode_str = "unknown"
        else:
            game_mode_str = getattr(game_mode, "name", str(game_mode))
        result = result.replace("{gamemode}", str(game_mode_str))

        address = getattr(player, "address", None)
        if address is None:
            ip_value = "unknown"
        else:
            ip_value = getattr(address, "hostname", None) or str(address)
        result = result.replace("{ip}", ip_value)

        if self.time_based_greetings_enabled and "{greeting}" in result:
            result = result.replace("{greeting}", self.get_time_based_greeting())

        if self.join_statistics_enabled:
            player_key = str(player.unique_id).lower()
            join_count = self.player_join_counts.get(player_key, 0)
            result = result.replace("{joincount}", str(join_count)).replace("{joins}", str(join_count))

        if self.welcome_back_timer_enabled and "{lastseen}" in result:
            player_key = str(player.unique_id).lower()
            last_seen_value = last_seen_override
            if last_seen_value is None:
                last_seen_value = self.player_last_seen.get(player_key)

            if last_seen_value:
                result = result.replace("{lastseen}", self.format_time_since(last_seen_value))
            else:
                result = result.replace("{lastseen}", "first time")

        if "{playtime}" in result:
            player_key = str(player.unique_id).lower()
            total_playtime = self.player_total_playtime.get(player_key, 0)
            result = result.replace("{playtime}", self.format_playtime(total_playtime))

        if self.player_count_enabled:
            online = len(self._plugin.server.online_players)
            max_players = self._plugin.server.max_players
            if "{online}" in result or "{max}" in result or "{count}" in result:
                online_str = str(online)
                max_str = str(max_players)
                result = (
                    result.replace("{online}", online_str)
                    .replace("{max}", max_str)
                    .replace("{count}", f"[{online_str}/{max_str}]")
                )

        return colorize(result)

    def cleanup_old_join_times(self) -> None:
        current_time = int(time.time() * 1000)
        cutoff_time = current_time - self.JOIN_TIME_RETENTION_MS
        self.last_join_time = {
            key: value for key, value in self.last_join_time.items() if value >= cutoff_time
        }

        inactive_cutoff = current_time - self.INACTIVE_RETENTION_MS
        self.awarded_join_milestones = {
            key: value
            for key, value in self.awarded_join_milestones.items()
            if self.player_last_seen.get(key, current_time) >= inactive_cutoff
        }
        self.awarded_playtime_milestones = {
            key: value
            for key, value in self.awarded_playtime_milestones.items()
            if self.player_last_seen.get(key, current_time) >= inactive_cutoff
        }
        if self._legacy_last_seen:
            self._legacy_awarded_join_milestones = {
                key: value
                for key, value in self._legacy_awarded_join_milestones.items()
                if self._legacy_last_seen.get(key, current_time) >= inactive_cutoff
            }
            self._legacy_awarded_playtime_milestones = {
                key: value
                for key, value in self._legacy_awarded_playtime_milestones.items()
                if self._legacy_last_seen.get(key, current_time) >= inactive_cutoff
            }

    def get_time_based_greeting(self) -> str:
        hour = datetime.datetime.now().hour
        if 5 <= hour < 12:
            return "Good morning"
        if 12 <= hour < 17:
            return "Good afternoon"
        if 17 <= hour < 21:
            return "Good evening"
        return "Good night"

    def format_time_since(self, timestamp_ms: int) -> str:
        diff = int(time.time() * 1000) - timestamp_ms
        seconds = max(diff // 1000, 0)
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24

        if days > 0:
            return f"{days} day" + ("s" if days > 1 else "") + " ago"
        if hours > 0:
            return f"{hours} hour" + ("s" if hours > 1 else "") + " ago"
        if minutes > 0:
            return f"{minutes} minute" + ("s" if minutes > 1 else "") + " ago"
        return "just now"

    def format_playtime(self, milliseconds: int) -> str:
        hours = milliseconds // 3600000
        minutes = (milliseconds % 3600000) // 60000

        if hours > 0:
            return f"{hours}h {minutes}m"
        if minutes > 0:
            return f"{minutes}m"
        return "<1m"

    def _auto_announcement_tick(self) -> None:
        if not self.auto_announcements_enabled or not self.announcements:
            return

        now = time.time()
        if self.next_announcement_at is None:
            self.next_announcement_at = now + self.announcement_interval_seconds
            return
        if now < self.next_announcement_at:
            return

        self.next_announcement_at = now + self.announcement_interval_seconds
        self.send_auto_announcement()

    def send_auto_announcement(self) -> None:
        if not self.announcements:
            return
        if not self._plugin.server.online_players:
            return

        message = self.announcements[self.current_announcement_index]
        self.current_announcement_index = (self.current_announcement_index + 1) % len(self.announcements)

        message = message.replace("{online}", str(len(self._plugin.server.online_players))).replace(
            "{max}", str(self._plugin.server.max_players)
        )

        self._plugin.server.broadcast_message(colorize(message))

    def check_milestones(self, player: Player) -> None:
        if not self.player_milestones_enabled:
            return

        player_key = str(player.unique_id).lower()

        if player_key not in self.awarded_join_milestones:
            self.awarded_join_milestones[player_key] = set()
        if player_key not in self.awarded_playtime_milestones:
            self.awarded_playtime_milestones[player_key] = set()

        safe_name = sanitize_player_name(player.name)
        safe_command_name = format_player_for_command(player.name)
        safe_uuid = str(player.unique_id)

        join_count = self.player_join_counts.get(player_key, 0)
        for index, milestone in enumerate(self.milestone_joins):
            if join_count >= milestone and milestone not in self.awarded_join_milestones[player_key]:
                message = colorize(
                    f"&6&l* &e{player.name} &6has reached &e{milestone} joins &6milestone! &6&l*"
                )
                self._plugin.server.broadcast_message(message)
                self.awarded_join_milestones[player_key].add(milestone)
                self._pending_player_data_save = True
                self._dirty_players.add(player_key)

                if index < len(self.milestone_join_rewards):
                    reward = (
                        self.milestone_join_rewards[index]
                        .replace("{player}", safe_command_name)
                        .replace("{uuid}", safe_uuid)
                    )
                    self._plugin.logger.info(
                        f"Executing milestone reward command for player: {safe_name}"
                    )
                    self._plugin.server.scheduler.run_task(
                        self._plugin,
                        lambda cmd=reward: self._plugin.server.dispatch_command(
                            self._plugin.server.command_sender, cmd
                        ),
                    )

        playtime_hours = self.player_total_playtime.get(player_key, 0) // 3600000
        for index, milestone in enumerate(self.milestone_playtime_hours):
            if (
                playtime_hours >= milestone
                and milestone not in self.awarded_playtime_milestones[player_key]
            ):
                message = colorize(
                    f"&6&l* &e{player.name} &6has played for &e{milestone} hours&6! &6&l*"
                )
                self._plugin.server.broadcast_message(message)
                self.awarded_playtime_milestones[player_key].add(milestone)
                self._pending_player_data_save = True
                self._dirty_players.add(player_key)

                if index < len(self.milestone_playtime_rewards):
                    reward = (
                        self.milestone_playtime_rewards[index]
                        .replace("{player}", safe_command_name)
                        .replace("{uuid}", safe_uuid)
                    )
                    self._plugin.logger.info(
                        f"Executing playtime reward command for player: {safe_name}"
                    )
                    self._plugin.server.scheduler.run_task(
                        self._plugin,
                        lambda cmd=reward: self._plugin.server.dispatch_command(
                            self._plugin.server.command_sender, cmd
                        ),
                    )

    def send_preview(self, player: Player, preview_type: str) -> None:
        if preview_type.lower() == "join":
            message = self.get_join_message_for_player(player)
            message = self.format_message(
                message, player, self.player_last_seen.get(str(player.unique_id).lower())
            )
            player.send_message(colorize("&6=== Join Message Preview ==="))
            player.send_message(message)
            return

        if preview_type.lower() == "leave":
            message = self.get_leave_message_for_player(player)
            message = self.format_message(
                message, player, self.player_last_seen.get(str(player.unique_id).lower())
            )
            player.send_message(colorize("&6=== Leave Message Preview ==="))
            player.send_message(message)
            return

        player.send_error_message("Invalid type. Use 'join' or 'leave'.")


class EuphoriaMessagesPlugin(Plugin):
    VERSION = "1.0.1"
    UPDATE_CHECK_TICK_SECONDS = 60
    prefix = "EuphoriaMessages"
    api_version = "0.6"
    load = "POSTWORLD"
    authors = ["Rep Graphics"]
    description = "Advanced join/leave messages"
    website = "https://github.com/EuphoriaDevelopmentOrg/EuphoriaMessages-Endstone"

    commands = {
        "euphoriamessages": {
            "description": "EuphoriaMessages main command",
            "usages": [
                "/euphoriamessages <reload|help|info|broadcast|preview>",
                "/euphoriamessages broadcast <message>",
                "/euphoriamessages preview <join|leave>",
            ],
            "aliases": ["em", "emsg"],
            "permissions": ["euphoriamessages.command"],
        }
    }

    permissions = {
        "euphoriamessages.*": {
            "description": "All EuphoriaMessages permissions",
            "default": "op",
            "children": {
                "euphoriamessages.command": True,
                "euphoriamessages.reload": True,
                "euphoriamessages.broadcast": True,
                "euphoriamessages.role.moderator": True,
                "euphoriamessages.role.vip": True,
                "euphoriamessages.role.builder": True,
                "euphoriamessages.role.helper": True,
            },
        },
        "euphoriamessages.command": {
            "description": "Access to /euphoriamessages command",
            "default": True,
        },
        "euphoriamessages.reload": {
            "description": "Reload the configuration",
            "default": "op",
        },
        "euphoriamessages.broadcast": {
            "description": "Broadcast messages to all players",
            "default": "op",
        },
        "euphoriamessages.silent": {
            "description": "Join and leave silently without messages",
            "default": False,
        },
        "euphoriamessages.role.moderator": {
            "description": "Use moderator join/leave messages",
            "default": "op",
        },
        "euphoriamessages.role.vip": {
            "description": "Use VIP join/leave messages",
            "default": False,
        },
        "euphoriamessages.role.builder": {
            "description": "Use builder join/leave messages",
            "default": False,
        },
        "euphoriamessages.role.helper": {
            "description": "Shows helper tag in join/leave messages",
            "default": False,
        },
    }

    def __init__(self) -> None:
        super().__init__()
        self._listener: Optional[WelcomeMessageListener] = None
        self._update_checker_enabled = False
        self._update_repository = "EuphoriaDevelopmentOrg/EuphoriaMessages-Endstone"
        self._update_interval_seconds = 0
        self._update_notify_no_update = False
        self._update_run_once = False
        self._update_in_progress = False
        self._update_failures = 0
        self._update_backoff_until: Optional[float] = None
        self._next_update_check_at: Optional[float] = None
        self._update_checker_task_started = False

    def on_enable(self) -> None:
        self.save_default_config()
        self.reload_config()

        self._listener = WelcomeMessageListener(self)
        self.register_events(self._listener)

        self.get_command("euphoriamessages").executor = EuphoriaMessagesCommandExecutor(self)
        self._configure_update_checker(initial=True)

        self.logger.info(f"EuphoriaMessages v{self.VERSION} enabled successfully!")

    def on_disable(self) -> None:
        if self._listener is not None:
            self._listener.save_pending_data()
        self.logger.info("EuphoriaMessages disabled")

    def reload_configuration(self) -> None:
        self.reload_config()
        if self._listener is not None:
            self._listener.load_config()
        self._configure_update_checker(initial=False)
        self.logger.info("Configuration reloaded successfully!")

    def broadcast_message(self, message: str, sender: CommandSender) -> None:
        formatted = colorize(message)
        self.server.broadcast_message(formatted)
        player = sender.as_player() if hasattr(sender, "as_player") else None
        if player is not None:
            player.send_message(colorize("&aBroadcast sent!"))

    def preview_message(self, player: Player, preview_type: str) -> None:
        if self._listener is not None:
            self._listener.send_preview(player, preview_type)

    def send_help(self, sender: CommandSender) -> None:
        sender.send_message(colorize("&6=== EuphoriaMessages Commands ==="))
        sender.send_message(colorize("&e/em help &f- Show this help message"))
        sender.send_message(colorize("&e/em reload &f- Reload configuration"))
        sender.send_message(colorize("&e/em info &f- Show plugin information"))
        sender.send_message(colorize("&e/em broadcast <msg> &f- Broadcast a message"))
        sender.send_message(colorize("&e/em preview <join|leave> &f- Preview your message"))

    def _configure_update_checker(self, initial: bool) -> None:
        config = self.config.get("update-checker", {})
        if not isinstance(config, dict):
            config = {}

        enabled = bool(config.get("enabled", True))
        repository = str(config.get("repository", self._update_repository)).strip()
        interval_hours = WelcomeMessageListener._as_int(config.get("interval-hours", 12), 12)
        notify_no_update = bool(config.get("notify-no-update", False))

        if not repository:
            repository = self._update_repository

        if interval_hours < 1:
            interval_hours = 0

        self._update_checker_enabled = enabled
        self._update_repository = repository
        self._update_interval_seconds = interval_hours * 3600
        self._update_notify_no_update = notify_no_update
        self._update_run_once = interval_hours == 0
        self._update_failures = 0
        self._update_backoff_until = None
        self._update_in_progress = False

        if not self._update_checker_task_started:
            self.server.scheduler.run_task(
                self,
                self._update_checker_tick,
                delay=20,
                period=20 * self.UPDATE_CHECK_TICK_SECONDS,
            )
            self._update_checker_task_started = True

        if self._update_checker_enabled:
            self._next_update_check_at = 0 if initial else time.time()
        else:
            self._next_update_check_at = None

    def _update_checker_tick(self) -> None:
        if not self._update_checker_enabled:
            return

        if self._update_run_once and self._next_update_check_at is None:
            return

        if self._update_in_progress:
            return

        now = time.time()
        if self._next_update_check_at is None:
            self._next_update_check_at = now

        if self._update_backoff_until is not None:
            if now < self._update_backoff_until:
                return
            self._update_backoff_until = None

        if now < self._next_update_check_at:
            return

        if self._update_interval_seconds > 0:
            self._next_update_check_at = now + self._update_interval_seconds
        else:
            self._next_update_check_at = None

        self._update_in_progress = True
        self.server.scheduler.run_task(
            self,
            lambda: self._run_update_check_async(),
            delay=0,
        )

    def _run_update_check_async(self) -> None:
        import threading

        thread = threading.Thread(target=self._run_update_check, daemon=True)
        thread.start()

    def _run_update_check(self) -> None:
        try:
            success = self._check_for_updates()
            if success:
                self._update_failures = 0
                self._update_backoff_until = None
            else:
                self._register_update_failure("No update data returned.")
        except UpdateCheckError as exc:
            self._register_update_failure(str(exc), status=exc.status)
        except Exception as exc:
            self._register_update_failure(str(exc))
        finally:
            self._update_in_progress = False

    def _register_update_failure(self, reason: str, status: Optional[int] = None) -> None:
        self._update_failures += 1
        if status in {403, 429}:
            backoff_seconds = max(3600, min(6 * 3600, 60 * (2 ** (self._update_failures - 1))))
        else:
            backoff_seconds = min(6 * 3600, 60 * (2 ** (self._update_failures - 1)))
        self._update_backoff_until = time.time() + backoff_seconds
        if self._update_failures == 1 or status in {403, 429}:
            self.logger.warning(
                f"Update check failed: {reason}. Retrying in {int(backoff_seconds)}s."
            )

    def _check_for_updates(self) -> bool:
        repository = self._update_repository
        if not repository:
            return False

        latest_version, latest_url = self._fetch_latest_release(repository)
        if not latest_version:
            return False

        if _is_newer_version(self.VERSION, latest_version):
            message = (
                f"Update available: v{latest_version} (current v{self.VERSION}). "
                f"Download: {latest_url}"
            )
            self.logger.info(message)
        elif self._update_notify_no_update:
            self.logger.info("EuphoriaMessages is up to date.")

        return True

    def _fetch_latest_release(self, repository: str) -> tuple[Optional[str], str]:
        latest_url = f"https://github.com/{repository}/releases/latest"
        last_error: Optional[Exception] = None
        try:
            data = self._fetch_github_json(
                f"https://api.github.com/repos/{repository}/releases/latest"
            )
            if isinstance(data, dict):
                tag = data.get("tag_name") or data.get("name")
                if tag:
                    return str(tag), str(data.get("html_url") or latest_url)
        except Exception as exc:
            last_error = exc

        try:
            data = self._fetch_github_json(
                f"https://api.github.com/repos/{repository}/tags?per_page=1"
            )
            if isinstance(data, list) and data:
                tag = data[0].get("name")
                if tag:
                    tag_url = f"https://github.com/{repository}/releases/tag/{tag}"
                    return str(tag), tag_url
        except Exception as exc:
            last_error = exc

        if last_error is not None:
            raise last_error

        return None, latest_url

    def _fetch_github_json(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "EuphoriaMessages-Endstone",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                payload = response.read().decode("utf-8")
            return json.loads(payload)
        except urllib.error.HTTPError as exc:
            raise UpdateCheckError(f"GitHub API error {exc.code}", status=exc.code) from exc
        except urllib.error.URLError as exc:
            raise UpdateCheckError(f"Network error: {exc.reason}") from exc
        except Exception as exc:
            raise UpdateCheckError(str(exc)) from exc
