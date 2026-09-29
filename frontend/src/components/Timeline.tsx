import { useEffect, useRef } from "react";
import type { Event } from "../types";

export function Timeline({ events }: { events: Event[] }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (ref.current) ref.current.scrollTop = ref.current.scrollHeight;
  }, [events.length]);

  return (
    <div className="timeline" ref={ref}>
      {events.length === 0 && <p className="hint">事件将在这里出现：选举、消息丢弃、提交、拒绝。</p>}
      {events.map((ev, i) => (
        <div key={i} className={`timeline-item kind-${ev.kind}`}>
          <span className="tick">t={ev.tick}</span>
          <span className={`kind-tag tag-${ev.kind}`}>{ev.kind}</span>
          <span className="event-text">{ev.text}</span>
        </div>
      ))}
    </div>
  );
}
