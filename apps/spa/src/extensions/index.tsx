import { registerExtension } from "./registry";
import TypeFacets from "./typeFacets";
import CategoryMap from "./CategoryMap";
import RelatedEntities from "./RelatedEntities";

registerExtension({
  id: "ext-type-facets",
  title: "Type facets",
  slot: "category.sidebar",
  render: ({ categoryId }) => <TypeFacets categoryId={categoryId} />,
});

registerExtension({
  id: "category.map",
  title: "Category map",
  slot: "category.sidebar",
  render: ({ categoryId }) => <CategoryMap categoryId={categoryId} />,
});

registerExtension({
  id: "entity.related",
  title: "Related entities",
  slot: "entity.sidebar",
  render: ({ entityId }) => <RelatedEntities entityId={entityId} />,
});
