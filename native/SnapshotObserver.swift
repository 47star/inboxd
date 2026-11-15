import EventKit
import Foundation

final class SnapshotObserver {
    typealias Fetch = (@escaping ([String: Any]) -> Void) -> Void

    private let fetch: Fetch
    private let pollSeconds: TimeInterval
    private let queue = DispatchQueue(label: "net.donghoon.inboxd.snapshot")
    private let lock = NSLock()
    private var generation = 0
    private var running = false
    private var dirty = false
    private var observer: NSObjectProtocol?
    private var timer: Timer?

    init(pollSeconds: TimeInterval = 60, fetch: @escaping Fetch) {
        self.pollSeconds = pollSeconds
        self.fetch = fetch
    }

    func start() {
        observer = NotificationCenter.default.addObserver(
            forName: .EKEventStoreChanged,
            object: nil,
            queue: nil
        ) { [weak self] _ in
            self?.request(changed: true)
        }

        timer = Timer.scheduledTimer(withTimeInterval: pollSeconds, repeats: true) {
            [weak self] _ in
            self?.request(changed: false)
        }

        request(changed: false)
    }

    private func request(changed: Bool) {
        lock.lock()

        if changed {
            generation += 1
            dirty = true
        }

        if running {
            lock.unlock()
            return
        }

        running = true
        dirty = false
        let revision = generation
        lock.unlock()

        queue.async {
            self.fetch { snapshot in
                self.queue.async {
                    self.finish(snapshot, revision: revision)
                }
            }
        }
    }

    private func finish(_ snapshot: [String: Any], revision: Int) {
        lock.lock()
        let stable = generation == revision
        let refresh = dirty
        lock.unlock()

        var result = snapshot
        result["snapshot_id"] = UUID().uuidString
        result["complete"] = stable && (snapshot["complete"] as? Bool == true)
        result["changed_during_read"] = !stable
        writeJSON(result)

        lock.lock()
        running = false
        let changedAfterWrite = dirty
        lock.unlock()

        if refresh || changedAfterWrite {
            queue.asyncAfter(deadline: .now() + 0.5) {
                self.request(changed: false)
            }
        }
    }
}
