import { useEffect } from "react";

export function useJsonLd(data: object, id: string) {
  useEffect(() => {
    const scriptId = `jsonld-${id}`;
    let script = document.getElementById(scriptId) as HTMLScriptElement | null;

    if (!script) {
      script = document.createElement("script");
      script.type = "application/ld+json";
      script.id = scriptId;
      document.head.appendChild(script);
    }

    script.textContent = JSON.stringify(data);

    return () => {
      const el = document.getElementById(scriptId);
      if (el) el.remove();
    };
  }, [data, id]);
}
