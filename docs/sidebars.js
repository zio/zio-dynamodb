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
            "concepts/high-level-api",
            "concepts/low-level-api",
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
            "guides/data-modelling",
            "guides/codec-customization",
            "guides/transactions",
            "guides/testing",
            "guides/ce-interop",
          ]
        },
        // Reference
        {
          type: "category",
          collapsed: false,
          label: "Reference",
          items: [
            {
              type: "category",
              label: "High Level API",
              collapsed: true,
              link: { type: "doc", id: "reference/hi-level-api/index" },
              items: [
                {
                  type: "category",
                  label: "Creating Models",
                  collapsed: true,
                  link: { type: "doc", id: "reference/hi-level-api/creating-models/index" },
                  items: [
                    "reference/hi-level-api/creating-models/field-traversal",
                  ]
                },
                {
                  type: "category",
                  label: "CRUD Operations",
                  collapsed: true,
                  link: { type: "doc", id: "reference/hi-level-api/crud-operations/index" },
                  items: [
                    "reference/hi-level-api/crud-operations/put",
                    "reference/hi-level-api/crud-operations/get",
                    "reference/hi-level-api/crud-operations/update",
                    "reference/hi-level-api/crud-operations/delete",
                  ]
                },
                {
                  type: "category",
                  label: "Scan and Query Operations",
                  collapsed: true,
                  link: { type: "doc", id: "reference/hi-level-api/scan-and-query-operations/index" },
                  items: [
                    "reference/hi-level-api/scan-and-query-operations/scan-all",
                    "reference/hi-level-api/scan-and-query-operations/scan-some",
                    "reference/hi-level-api/scan-and-query-operations/query-all",
                    "reference/hi-level-api/scan-and-query-operations/query-some",
                  ]
                },
                "reference/hi-level-api/primary-keys",
              ]
            },
            {
              type: "category",
              label: "Low Level API",
              collapsed: false,
              link: { type: "doc", id: "reference/low-level-api/index" },
              items: [
                "reference/low-level-api/attribute-value",
                "reference/low-level-api/dollar-function",
              ]
            },
            "reference/dynamodb-query",
            "reference/projection-expression",
            "reference/error-handling",
            "reference/auto-batching-and-parallelisation",
            "reference/zio-dynamodb-json",
          ]
        }
      ]
    }
  ]
};

module.exports = sidebars;