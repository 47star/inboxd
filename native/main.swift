import AppKit
import EventKit
import Foundation

let arguments = CommandLine.arguments

guard arguments.count >= 2 else {
    fail("Expected calendar, reminders or notes")
}

let source = arguments[1]
let watching = arguments.contains("--watch")
var pollSeconds: TimeInterval = 60

if let index = arguments.firstIndex(of: "--poll-seconds") {
    guard index + 1 < arguments.count,
        let value = TimeInterval(arguments[index + 1]),
        value.isFinite, value >= 5
    else {
        fail("--poll-seconds must be at least 5")
    }

    pollSeconds = value
}

if source == "notes" {
    guard arguments.count == 3 else {
        fail("Notes script path required")
    }

    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/osascript")
    process.arguments = ["-l", "JavaScript", arguments[2]]
    process.standardOutput = FileHandle.standardOutput
    process.standardError = FileHandle.standardError

    do {
        try process.run()
        process.waitUntilExit()
        exit(process.terminationStatus)
    } catch {
        fail(String(describing: error))
    }
}

let store = EKEventStore()
var observer: SnapshotObserver?

func begin(_ fetch: @escaping SnapshotObserver.Fetch) {
    DispatchQueue.main.async {
        if watching {
            observer = SnapshotObserver(pollSeconds: pollSeconds, fetch: fetch)
            observer?.start()
        } else {
            fetch { snapshot in
                writeJSON(snapshot)
                exit(0)
            }
        }
    }
}

if source == "calendar" {
    let startYear = arguments.count > 2 ? Int(arguments[2]) ?? 1970 : 1970
    let endYear = arguments.count > 3 ? Int(arguments[3]) ?? 2032 : 2032

    store.requestFullAccessToEvents { granted, error in
        guard granted else {
            fail(error?.localizedDescription ?? "Calendar access denied", status: 3)
        }

        begin { completion in
            completion(calendarSnapshot(store: store, startYear: startYear, endYear: endYear))
        }
    }
} else if source == "reminders" {
    store.requestFullAccessToReminders { granted, error in
        guard granted else {
            fail(error?.localizedDescription ?? "Reminders access denied", status: 3)
        }

        begin { completion in
            reminderSnapshot(store: store, completion: completion)
        }
    }
} else {
    fail("Unknown source")
}

RunLoop.main.run()
