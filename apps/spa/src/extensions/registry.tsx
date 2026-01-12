import type { ReactElement } from "react";

export type ExtensionSlot = "category.sidebar" | "entity.sidebar";

export type CategoryContext = { categoryId: string };
export type EntityContext = { entityId: string };

export type ExtensionContext = CategoryContext | EntityContext;

export type Extension = {
  id: string;
  title: string;
  slot: ExtensionSlot;
  render: (ctx: any) => ReactElement; // pragmatic: slot determines ctx shape
};

const extensions: Extension[] = [];

export function registerExtension(ext: Extension) {
  extensions.push(ext);
}

export function getExtensions(slot: ExtensionSlot): Extension[] {
  return extensions.filter((e) => e.slot === slot);
}
