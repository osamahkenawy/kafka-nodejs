import avro from 'avsc';

export default avro.Type.forSchema({
  type: 'record',
  fields: [
    {
      name: 'name',
      type: 'string',
    },
    {
      name: 'language',
      type: { type: 'enum', symbols: ['JAVASCRIPT', 'PYTHON', 'JAVA', 'CPP', 'RUBY'] }
    },
    {
      name: 'experience',
      type: 'string',
    },
    {
      name: 'timestamp',
      type: 'long',
    },
    {
      name: 'attributes',
      type: {
        type: 'record',
        fields: [
          {
            name: 'level',
            type: { type: 'enum', symbols: ['JUNIOR', 'MID', 'SENIOR'] }
          },
          {
            name: 'location',
            type: 'string'
          }
        ]
      }
    },
    {
      name: 'tags',
      type: {
        type: 'array',
        items: 'string'
      }
    }
  ]
});
