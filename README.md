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

### Commands

```sh
law run cf.PlotVariables1D \
 --version YOUR_VERSION \
 --processes zz_llll,ggf,h_ggf_4l,h_vbf_4l,wph_4l,wmh_4l,zh_4l,tth_4l \
 --datasets zz_llll_powheg,ggf_4e_mcfm,ggf_4mu_mcfm,ggf_4tau_mcfm,ggf_2e2mu_mcfm,ggf_2e2tau_mcfm,ggf_2mu2tau_mcfm,h_ggf_4l_powheg,h_vbf_4l_powheg,wph_4l_powheg,wmh_4l_powheg,zh_4l_powheg,tth_4l_powheg \
 --categories 4e,2e2mu,4mu \
 --hist-producer all_weights \
 --workers 16 \
 --variables m4l,mzz,mz1,mz2,m4l_zoomed,mzz_zoomed,n_electron,n_muon \
 --cms-label simpw
 --skip-ratio
```

```sh
law run cf.CreateYieldTable \
 --version YOUR_VERSION \
 --processes zz_llll,ggf,h_ggf_4l,h_vbf_4l,wph_4l,wmh_4l,zh_4l,tth_4l \
 --datasets zz_llll_powheg,ggf_4e_mcfm,ggf_4mu_mcfm,ggf_4tau_mcfm,ggf_2e2mu_mcfm,ggf_2e2tau_mcfm,ggf_2mu2tau_mcfm,h_ggf_4l_powheg,h_vbf_4l_powheg,wph_4l_powheg,wmh_4l_powheg,zh_4l_powheg,tth_4l_powheg \
 --categories 4e,2e2mu,4mu \
 --hist-producer all_weights \
 --workers 16
```
