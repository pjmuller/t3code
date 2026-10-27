// @effect-diagnostics globalDate:off - Form fields use the device's local calendar, outside an Effect runtime.
const pad = (value: number) => String(value).padStart(2, "0");

export function dateField(date: Date): string {
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** Forms edit a one-time run as a "YYYY-MM-DD" date and "HH:MM" time on this device's clock. */
export function onceRunAtFields(at: string): { readonly date: string; readonly time: string } {
  const date = new Date(at);
  if (Number.isNaN(date.getTime())) return { date: "", time: "09:00" };
  return { date: dateField(date), time: `${pad(date.getHours())}:${pad(date.getMinutes())}` };
}

export function tomorrowDateField(nowMs: number): string {
  const date = new Date(nowMs);
  date.setDate(date.getDate() + 1);
  return dateField(date);
}

/** `saved` is kept while the fields still show it, so its seconds survive an unrelated edit. */
export function onceRunAt(date: string, time: string, saved?: string | null): string | null {
  const day = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date);
  const clock = /^([01]?\d|2[0-3]):([0-5]\d)$/.exec(time);
  if (!day || !clock) return null;
  if (saved) {
    const fields = onceRunAtFields(saved);
    if (fields.date === date && fields.time === time) return saved;
  }
  const [year, month, dayOfMonth] = [Number(day[1]), Number(day[2]) - 1, Number(day[3])];
  const at = new Date(year, month, dayOfMonth, Number(clock[1]), Number(clock[2]));
  if (at.getMonth() !== month || at.getDate() !== dayOfMonth) return null;
  return at.toISOString();
}
