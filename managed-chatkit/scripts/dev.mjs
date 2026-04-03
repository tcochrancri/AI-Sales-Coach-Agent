import { spawn } from "node:child_process";

function start(name, args) {
  const child =
    process.platform === "win32"
      ? spawn("cmd.exe", ["/d", "/s", "/c", `npm.cmd ${args.join(" ")}`], {
          stdio: "inherit",
          shell: false,
        })
      : spawn("npm", args, {
          stdio: "inherit",
          shell: false,
        });

  child.on("error", (error) => {
    console.error(`[dev:${name}] failed to start: ${error.message}`);
  });

  return child;
}

const backend = start("backend", ["run", "backend"]);
const frontend = start("frontend", ["run", "frontend"]);

let shuttingDown = false;

function shutdown(signal = "SIGTERM") {
  if (shuttingDown) return;
  shuttingDown = true;
  if (!backend.killed) backend.kill(signal);
  if (!frontend.killed) frontend.kill(signal);
}

backend.on("exit", (code, signal) => {
  if (!shuttingDown) {
    console.error(`[dev:backend] exited (code=${code ?? "null"} signal=${signal ?? "null"})`);
    shutdown();
    process.exit(code ?? 1);
  }
});

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
