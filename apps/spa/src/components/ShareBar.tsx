import { useEffect, useState } from "react";
import QRCode from "qrcode";

export default function ShareBar({ title }: { title: string }) {
  const [qr, setQr] = useState<string>("");

  useEffect(() => {
    const url = window.location.href;
    QRCode.toDataURL(url, { margin: 1, width: 160 })
      .then(setQr)
      .catch(() => setQr(""));
  }, []);

  async function copy() {
    await navigator.clipboard.writeText(window.location.href);
    alert("Link copied!");
  }

  return (
    <div style={{ display: "flex", gap: 12, alignItems: "center", marginTop: 14 }}>
      <button
        onClick={copy}
        style={{ border: "1px solid #ddd", padding: "8px 10px", borderRadius: 10, cursor: "pointer" }}
      >
        Copy link
      </button>

      {qr && (
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <img src={qr} alt={`QR code for ${title}`} style={{ width: 90, height: 90 }} />
          <div style={{ fontSize: 12, opacity: 0.7, maxWidth: 360 }}>
            Scan to open this page
          </div>
        </div>
      )}
    </div>
  );
}
