# OBS-MCP Development Guidelines

## Build and Development Commands
- Build: `npm run build` - Compiles TypeScript and sets executable permissions
- Native build: `npm run native:build` - Builds the Rust entry and TypeScript compatibility payload offline
- Start: `npm run start` - Runs the built Rust MCP entry
- Compatibility start: `npm run start:compat` - Runs the existing TypeScript MCP server
- Native verification: `npm run native:check` - Checks Rust, the frozen contract, and cross-runtime parity

## Code Style Guidelines
### TypeScript
- **Imports**: ES modules with `.js` extensions in import paths
- **Formatting**: 2-space indentation, semicolons
- **Types**: Use strict TypeScript typing with interfaces, enums, and type annotations
- **Error Handling**: Wrap with try/catch blocks, include original error messages using pattern:
  `error instanceof Error ? error.message : String(error)`
- **Logging**: Use `logger` from `src/logger.ts` (stderr; stdout carries MCP). `OBS_MCP_LOG_LEVEL` sets the level

## Naming Conventions
- **Variables/Functions**: camelCase
- **Classes/Interfaces**: PascalCase
- **Constants**: UPPER_CASE or camelCase