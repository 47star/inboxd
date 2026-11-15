function run() {
    const app = Application("Notes");
    const records = [];
    const errors = [];
    const notes = app.notes();
    for (const note of notes) {
        let id = "";
        try {
            id = note.id();
            const container = note.container();
            const locked = note.passwordProtected();
            records.push({
                id: id,
                title: note.name(),
                text: locked ? "" : note.body(),
                created: note.creationDate().toISOString(),
                modified: note.modificationDate().toISOString(),
                folder_id: container.id(),
                folder: container.name(),
                locked: locked,
                account: "Apple Notes"
            });
        } catch (error) {
            errors.push({id: id, error: String(error)});
        }
    }
    return JSON.stringify({items: records, complete: errors.length === 0, errors: errors});
}
