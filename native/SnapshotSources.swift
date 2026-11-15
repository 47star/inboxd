import AppKit
import EventKit
import Foundation

private let formatter: ISO8601DateFormatter = {
    let value = ISO8601DateFormatter()
    value.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return value
}()

func timestamp(_ value: Date?) -> String? {
    value.map { formatter.string(from: $0) }
}

func writeJSON(_ value: [String: Any]) {
    do {
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        FileHandle.standardOutput.write(data)
        FileHandle.standardOutput.write(Data([10]))
    } catch {
        FileHandle.standardError.write(Data("JSON encoding failed\n".utf8))
        exit(2)
    }
}

func fail(_ message: String, status: Int32 = 2) -> Never {
    writeJSON(["error": message, "complete": false])
    exit(status)
}

func calendarSnapshot(store: EKEventStore, startYear: Int, endYear: Int) -> [String: Any] {
    var records: [[String: Any]] = []
    var seen = Set<String>()
    let calendar = Calendar(identifier: .gregorian)
    let calendars = store.calendars(for: .event)
    for year in startYear..<endYear {
        guard let start = calendar.date(from: DateComponents(year: year, month: 1, day: 1)),
            let end = calendar.date(from: DateComponents(year: year + 1, month: 1, day: 1))
        else {
            return ["error": "Invalid calendar interval", "complete": false, "items": []]
        }
        let predicate = store.predicateForEvents(withStart: start, end: end, calendars: calendars)
        for event in store.events(matching: predicate) {
            let identifier =
                event.calendarItemIdentifier + ":"
                + (timestamp(event.occurrenceDate ?? event.startDate) ?? "")
            if !seen.insert(identifier).inserted {
                continue
            }
            var record: [String: Any] = [
                "id": identifier,
                "calendar_id": event.calendar.calendarIdentifier,
                "calendar": event.calendar.title,
                "account": event.calendar.source.title,
                "title": event.title ?? "",
                "text": event.notes ?? "",
                "all_day": event.isAllDay,
                "status": event.status.rawValue,
                "location": event.location ?? "",
                "has_recurrence": event.hasRecurrenceRules,
                "attendees": (event.attendees ?? []).map { person in
                    [
                        "name": person.name ?? "", "url": person.url.absoluteString,
                        "status": String(person.participantStatus.rawValue),
                    ]
                },
            ]
            record["start"] = timestamp(event.startDate)
            record["end"] = timestamp(event.endDate)
            record["modified"] = timestamp(event.lastModifiedDate)
            record["created"] = timestamp(event.creationDate)
            record["url"] = event.url?.absoluteString
            records.append(record)
        }
    }
    return [
        "items": records,
        "complete": true,
        "start_year": startYear,
        "end_year_exclusive": endYear,
        "calendars": calendars.map {
            ["id": $0.calendarIdentifier, "name": $0.title, "account": $0.source.title]
        },
    ]
}

func reminderSnapshot(store: EKEventStore, completion: @escaping ([String: Any]) -> Void) {
    let calendars = store.calendars(for: .reminder)
    let predicate = store.predicateForReminders(in: calendars)
    store.fetchReminders(matching: predicate) { reminders in
        guard let reminders else {
            completion(["error": "Reminders fetch returned no result", "complete": false])
            return
        }
        let records: [[String: Any]] = reminders.map { reminder in
            var record: [String: Any] = [
                "id": reminder.calendarItemIdentifier,
                "calendar_id": reminder.calendar.calendarIdentifier,
                "calendar": reminder.calendar.title,
                "account": reminder.calendar.source.title,
                "title": reminder.title ?? "",
                "text": reminder.notes ?? "",
                "completed": reminder.isCompleted,
                "priority": reminder.priority,
                "has_recurrence": reminder.hasRecurrenceRules,
            ]
            record["created"] = timestamp(reminder.creationDate)
            record["modified"] = timestamp(reminder.lastModifiedDate)
            record["completed_at"] = timestamp(reminder.completionDate)
            record["due"] = timestamp(
                reminder.dueDateComponents.flatMap { Calendar.current.date(from: $0) }
            )
            record["url"] = reminder.url?.absoluteString
            return record
        }
        completion([
            "items": records,
            "complete": true,
            "calendars": calendars.map {
                ["id": $0.calendarIdentifier, "name": $0.title, "account": $0.source.title]
            },
        ])
    }
}
