import typescript from '@rollup/plugin-typescript';
import dts from 'rollup-plugin-dts';
import del from 'rollup-plugin-delete';

export default [
  {
    input: 'src/index.ts',
    output: {
      dir: 'dist',
      format: 'cjs'
    },
    plugins: [
      del({
        targets: 'dist'
      }),
      typescript()
    ]
  },
  {
    input: 'dist/dts/index.d.ts',
    output: [
      {
        file: 'dist/index.d.ts',
        format: 'es'
      }
    ],
    plugins: [dts()]
  }
];
