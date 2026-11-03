# OBS-MCP Development Guidelines

## Build and Development Commands
- Source oracle build: `npm run build` - Compiles the development TypeScript oracle
- Native build: `npm run native:build` - Builds the Rust service offline
- Start: `npm run start` - Runs the built Rust MCP service
- Source oracle start: `npm run oracle:start` - Runs the independent development oracle
- Native verification: `npm run native:check` - Checks Rust, the frozen contract, TLS and resolver fixtures, and public parity
- Native package: `npm run pack:native` - Builds the native-only MCPB with corresponding source; Node is a packaging tool

The installed runtime is Rust. TypeScript and the SDK remain development and
qualification tools; no Node compatibility payload is included in the product.

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
