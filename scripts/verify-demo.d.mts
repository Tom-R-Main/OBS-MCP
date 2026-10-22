export type DemoVerification = { status: "pass" | "fail" | "unavailable"; sha256?: string; problems: string[]; unavailable: string[] };
export function verifyDemo(options: { file: string; reference: string; seconds: number; chapters?: string[]; checkpoints?: { at: number; reference: string }[] }): DemoVerification;
