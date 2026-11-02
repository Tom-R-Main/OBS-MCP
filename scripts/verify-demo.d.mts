export type DemoVerification = {
  status: "pass" | "fail" | "unavailable"; sha256?: string; problems: string[]; unavailable: string[];
  contentVerification: "requires-review";
  checks: { metadata: "pass" | "fail" | "unavailable"; strictDecode: "pass" | "fail" | "unavailable"; sampledContent: "pass" | "fail" | "unavailable" };
  format?: { codec: string; pixelFormat: string; colorTransfer: string; colorPrimaries: string; colorSpace: string; colorFidelity: "unverified" };
};
export function verifyDemo(options: { file: string; reference: string; seconds: number; chapters?: string[]; checkpoints?: { at: number; reference: string }[]; decodeTimeoutMs?: number }): DemoVerification;
