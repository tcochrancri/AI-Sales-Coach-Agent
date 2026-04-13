import { spawn } from "node:child_process";

function startNpm(name, args) {
  const child =
    process.platform === "win32"
      ? spawn("cmd.exe", ["/d", "/s", "/c", `npm.cmd ${args.join(" ")}`], {
          stdio: "inherit",
          shell: false,
        })
      : spawn("npm", args, { stdio: "inherit", shell: false });
  child.on("error", (error) => {
    console.error(`[dev:${name}] failed to start: ${error.message}`);
  });
  return child;
}

let backend = null;
let frontend = null;
let shuttingDown = false;
let backendRestartCount = 0;

function startBackend() {
  backend = startNpm("backend", ["run", "backend"]);
  backend.on("exit", (code, signal) => {
    if (shuttingDown) return;
    const codeLabel = code ?? "null";
    const signalLabel = signal ?? "null";
    console.error(`[dev:backend] exited (code=${codeLabel} signal=${signalLabel})`);
    backend = null;
    backendRestartCount += 1;
    const delayMs = 1000;
    console.error(`[dev] restarting backend in ${delayMs}ms (attempt ${backendRestartCount})...`);
    setTimeout(() => {
      if (!shuttingDown) startBackend();
    }, delayMs);
  });
}

function shutdown(signal = "SIGTERM") {
  if (shuttingDown) return;
  shuttingDown = true;
  if (backend && !backend.killed) backend.kill(signal);
  if (frontend && !frontend.killed) frontend.kill(signal);
}

startBackend();
frontend = startNpm("frontend", ["run", "frontend"]);

frontend.on("exit", (code, signal) => {
  if (!shuttingDown) {
    console.error(`[dev:frontend] exited (code=${code ?? "null"} signal=${signal ?? "null"})`);
    shutdown();
    process.exit(code ?? 1);
  }
});

process.on("SIGINT", () => {
  shutdown("SIGINT");
  process.exit(130);
});

process.on("SIGTERM", () => {
  shutdown("SIGTERM");
  process.exit(143);
});
