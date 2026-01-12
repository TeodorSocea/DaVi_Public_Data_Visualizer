// Base64url without padding (matches the backend)
export function uriToId(uri: string): string {
    const b64 = btoa(unescape(encodeURIComponent(uri)))
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/g, "");
    return b64;
  }
  
  export function idToUri(id: string): string {
    let b64 = id.replace(/-/g, "+").replace(/_/g, "/");
    while (b64.length % 4 !== 0) b64 += "=";
    return decodeURIComponent(escape(atob(b64)));
  }
  