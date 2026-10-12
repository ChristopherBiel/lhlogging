# Fonts

Faceplate uses **Manrope** (sans) and **IBM Plex Mono** (mono). Both are SIL OFL: free to self-host, embed and redistribute. The licences are in this folder.

**Web:** the `.woff2` files ship here, and `../fonts.css` declares them. Link it before the tokens:

```html
<link rel="stylesheet" href="faceplate/fonts.css">
<link rel="stylesheet" href="faceplate/dist/faceplate.tokens.css">
```

| File | Face |
|---|---|
| `manrope-variable.woff2` | Manrope, variable, weights 400–800 |
| `ibm-plex-mono-400.woff2` `-500` `-600` | IBM Plex Mono Regular, Medium, SemiBold |

Self-hosting keeps every request on your own domain. The Google Fonts CDN sends visitors' IP addresses to Google, which needs consent in the EU, so use it for prototypes only.

**Documents:** the LaTeX package ships Manrope as TTF in `latex/fonts/`; IBM Plex Mono comes with TeX Live.

The font stacks fall back to system fonts, so layouts never break if a file is missing.
