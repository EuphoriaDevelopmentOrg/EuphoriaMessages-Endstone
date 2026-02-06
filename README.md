# EuphoriaMessages (Endstone)

Advanced join/leave messages with roles, milestones, random variants, and auto announcements for Endstone servers.

## Install

1. Build or install the plugin:
   - Development: `pip install -e .`
   - Release: `pipx run build --wheel`, then copy the `.whl` from `dist/` into your server `plugins/` directory.
2. Start the server once to generate `config.toml`.

## Configure

Edit `config.toml` in the plugin data folder and reload:

```
/euphoriamessages reload
```

## Optional MySQL Storage

2. Set `player-data.storage = "mysql"` or `player-data.mysql.enabled = true` in `config.toml`.
3. Fill in the `[player-data.mysql]` connection details.
4. Restart or run `/euphoriamessages reload`.

## Update Checker

The plugin can check GitHub for new releases:

- Configure it under `[update-checker]` in `config.toml`.

## Commands

- `/euphoriamessages help` (alias: `/em`, `/emsg`)
- `/euphoriamessages reload`
- `/euphoriamessages info`
- `/euphoriamessages broadcast <message>`
- `/euphoriamessages preview <join|leave>`

## Permissions

- `euphoriamessages.command`
- `euphoriamessages.reload`
- `euphoriamessages.broadcast`
- `euphoriamessages.silent`
- `euphoriamessages.role.moderator`
- `euphoriamessages.role.vip`
- `euphoriamessages.role.builder`
- `euphoriamessages.role.helper`

## Placeholders

- `{player}`, `{name}`, `{displayname}`
- `{online}`, `{max}`, `{count}`
- `{ping}`, `{world}`, `{gamemode}`, `{ip}`
- `{greeting}`, `{joincount}`, `{joins}`, `{lastseen}`, `{playtime}`

## Notes

- The Endstone port stores player data in `playerdata.json` inside the plugin data folder.
- Color codes use `&` (they are converted to Minecraft formatting automatically).
