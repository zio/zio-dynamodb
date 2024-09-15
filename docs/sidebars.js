const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO DynamoDB",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        // Concepts
        {
          type: "category",
          collapsed: false,
          label: "Concepts",
          items: [
            "concepts/architecture",
            "concepts/low-level-api",
            "concepts/high-level-api",
            "concepts/transactions",
          ],

        },
        // Guides
        {
          type: "category",
          collapsed: false,
          label: "Guides",
          items: [
            "guides/getting-started",
            "guides/cheat-sheet",
            "guides/codec-customization"
          ]
        },
        // Reference
        {
          type: "category",
          collapsed: false,
          label: "Reference",
          items: [
            "reference/dynamodb-query",
            "reference/attribute-value",
            "reference/projection-expression",
          ]
        }
      ]
    }
  ]
};

module.exports = sidebars;