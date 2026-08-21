export type ToolSummary = {
  name: string;
  description?: string;
};

export function listTools(root: string): Promise<ToolSummary[]>;
