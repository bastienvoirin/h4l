# h4l Analysis

### Resources

- [columnflow](https://github.com/columnflow/columnflow/)
- [law](https://github.com/riga/law)
- [order](https://github.com/riga/order)
- [luigi](https://github.com/spotify/luigi)

### Base command

```sh
law run cf.PlotVariables1D --version YOUR_VERSION --processes zz_llll,h_ggf_4l --dataset zz_llll_powheg,h_ggf_4l_powheg --variables m4l --skip-ratio --workers 16
```

### Inclusive vs. `4e`/`2e2mu`/`4mu` categories

Inclusive by default, for separate `4e`/`2e2mu`/`4mu` categories use

```sh
--categories 4e,2e2mu,4mu
```

### Variables

```sh
--variables m4l
```

```sh
--variables m4l,mzz,mz1,mz2
```

### Backgrounds

```sh
--processes zz_llll,h_ggf_4l --dataset zz_llll_powheg,h_ggf_4l_powheg
```
