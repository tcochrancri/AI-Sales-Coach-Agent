import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const script =
  process.platform === "win32"
    ? path.join(__dirname, "run.ps1")
    : path.join(__dirname, "run.sh");

const command = process.platform === "win32" ? "powershell.exe" : script;
const args =
  process.platform === "win32"
    ? ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script]
    : [];

const child = spawn(command, args, { stdio: "inherit", shell: false });

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});

child.on("error", (err) => {
  console.error(`Failed to start backend script: ${err.message}`);
  process.exit(1);
});
