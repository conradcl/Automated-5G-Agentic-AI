import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from oran_agent.main import collect_system_state
from oran_agent.llm.deepseek_client import ask_deepseek_about_state

def print_help() -> None:
    print("\nAvailable commands:")
    print("  /help          Show this help menu")
    print("  /state         Print raw live system-state JSON")
    print("  /diagnostics   Print deterministic diagnostics JSON")
    print("  exit           Quit the terminal agent")
    print()
    print("Example questions:")
    print("  Is the testbed healthy right now?")
    print("  Is the UE connected?")
    print("  Are uplink and downlink traffic working?")
    print("  What evidence supports your answer?")
    print("  If the system is unhealthy, what should I check first?")

def save_chat_turn(
    log_path: Path,
    question: str,
    answer: str,
    state: Dict[str, Any]
) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "question": question,
        "answer": answer,
        "overall_health": state.get("overall_health"),
        "state": state
    }

    with log_path.open("a") as file:
        json.dump(record, file)
        file.write("\n")


def print_intro(log_path: Path) -> None:
    print("=" * 70)
    print("ORAN-Copilot Terminal Agent")
    print("=" * 70)
    print("Ask questions about the live OAI/FlexRIC 5G/O-RAN testbed.")
    print()
    print("Commands:")
    print("  /state  - print raw live system-state JSON")
    print("  /help   - show this help message")
    print("  /diagnostics   Print deterministic diagnostics JSON")
    print("  exit    - quit")
    print()
    print(f"Session log: {log_path}")
    print("=" * 70)


def main() -> None:
    runs_dir = Path("runs/chat_sessions")
    runs_dir.mkdir(parents=True, exist_ok=True)

    session_time = datetime.now(timezone.utc).isoformat().replace(":", "-")
    log_path = runs_dir / f"{session_time}.jsonl"

    print_intro(log_path)

    while True:
        try:
            question = input("\nORAN-Copilot> ").strip()
        except KeyboardInterrupt:
            print("\nExiting ORAN-Copilot.")
            break
        except EOFError:
            print("\nExiting ORAN-Copilot.")
            break

        if not question:
            continue

        if question.lower() in ["exit", "quit", "q"]:
            print("Exiting ORAN-Copilot.")
            break

        if question.lower() == "/help":
             print_help()
            continue

        print("\n[1/3] Collecting live testbed state...")
        state = collect_system_state()

        if question.lower() == "/state":
            print("\n=== Raw Live System State JSON ===")
            print(json.dumps(state, indent=2))
            save_chat_turn(
                log_path,
                question,
                "User requested raw system state JSON.",
                state
            )
            continue

        if question.lower() == "/diagnostics":
            print("\n=== Deterministic Diagnostics ===")
            print(json.dumps(state.get("diagnostics", {}), indent=2))
            save_chat_turn(
                log_path,
                question,
                "User requested deterministic diagnostics.",
                state
            )
            continue


        diagnostics = state.get("diagnostics", {})
        print(
            f"[2/3] Status = {diagnostics.get('status_label')} | "
            f"overall_health = {state.get('overall_health')}"
        )
        print("[3/3] Sending structured state to DeepSeek...")

        answer = ask_deepseek_about_state(state, question)

        print("\nAgent:")
        print(answer)

        save_chat_turn(log_path, question, answer, state)


if __name__ == "__main__":
    main()