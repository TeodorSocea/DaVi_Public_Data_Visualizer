export type ExtensionSlot = "category.sidebar" | "entity.sidebar";

export type CategoryContext = { categoryId: string };
export type EntityContext = { entityId: string };

export type ExtensionContextMap = {
  "category.sidebar": CategoryContext;
  "entity.sidebar": EntityContext;
};

// Generic Extension type: render() gets the right ctx for the slot
export type Extension<S extends ExtensionSlot = ExtensionSlot> = {
  id: string;
  title: string;
  slot: S;
  render: (ctx: ExtensionContextMap[S]) => JSX.Element;
};

const extensions: Extension[] = [];

export function registerExtension<S extends ExtensionSlot>(ext: Extension<S>) {
  extensions.push(ext as Extension);
}

export function getExtensions<S extends ExtensionSlot>(slot: S): Extension<S>[] {
  return extensions.filter((e) => e.slot === slot) as Extension<S>[];
}
