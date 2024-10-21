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
            "guides/codec-customization",
            "guides/testing"
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
                    "reference/hi-level-api/creating-models/optics",
                    "reference/hi-level-api/creating-models/collection-field-traversal"
                  ]
                },
                {
                  type: "category",
                  label: "CRUD Operations",
                  collapsed: false,
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
                "reference/hi-level-api/query-combinators",
              ]
            },
            {
              type: "category",
              label: "Low Level API",
              collapsed: true,
              link: { type: "doc", id: "reference/low-level-api/index" },
              items: [
                "reference/low-level-api/attribute-value",
                "reference/low-level-api/primary-keys",
              ]
            },
            "reference/dynamodb-query",
            "reference/projection-expression",
            "reference/error-handling",
            "reference/dynamodb-json"
          ]
        }
      ]
    }
  ]
};

/*
            {
              type: "category",
              label: "Codecs",
              collapsed: true,
              link: { type: "doc", id: "derivations/codecs/index" },
              items: [
                "derivations/codecs/avro",
                "derivations/codecs/thrift",
                "derivations/codecs/bson",
                "derivations/codecs/json",
                "derivations/codecs/message-pack",
                "derivations/codecs/protobuf",
              ],
            },
*/

module.exports = sidebars;