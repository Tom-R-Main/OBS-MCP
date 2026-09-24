# OBS-MCP Development Guidelines

## Build and Development Commands
- Build: `npm run build` - Compiles TypeScript and sets executable permissions
- Start: `npm run start` - Runs the MCP server for OBS Studio

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