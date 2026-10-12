%% fpletter — a letter in Faceplate. Personal level, intensity 01 unless you say otherwise, A4.
%% The sheet, the lead and the footer come from fpdoc, so a letter reads as a
%% pair with the CV. Labels (To, Date, Re) stand in the label column; the text
%% runs in the body and flows across pages.
%%   \documentclass{fpletter}                 01 Line
%%   \documentclass[intensity=02]{fpletter}   02 Tint
%%   \documentclass[window]{fpletter}         the address where a DIN 5008 (form B) window
%%                                            envelope shows it. Function wins over the grid
%%                                            there, and only there.
%% Rules: guidelines/applications/letter.md · How to use: guidelines/media/latex.md
\NeedsTeXFormat{LaTeX2e}[2022/11/01]
\ProvidesClass{fpletter}[2026/10/11 Faceplate letter]
\newif\iffpletter@window
\def\fpletter@intensity{01}
\DeclareKeys[fpletter]{ window.code = \fpletter@windowtrue, intensity.store = \fpletter@intensity }
\ProcessKeyOptions[fpletter]
\PassOptionsToClass{intensity=\fpletter@intensity, level=personal}{fpdoc}
\LoadClass{fpdoc}
\kind{Letter}
%% A letter is read through: ragged right, hyphenation allowed but discouraged.
\hyphenpenalty=500

\newcommand\recipient[1]{\def\fpletter@to{#1}}        \recipient{}
\newcommand\subject[1]{\def\fpletter@re{#1}}          \subject{}
\newcommand\returnaddress[1]{\def\fpletter@from{#1}}  \returnaddress{}
\date{\today}

%% \makeletterhead — the lead (contact lines in the label column, role and name
%% beside them), then a rule and the To, Date and Re rows. Everything after it
%% is the letter, set in the body column.
\newcommand\makeletterhead{%
  \fpdoc@lead{\fpdoc@stack}{\fpdoc@leadlabel{\fpdoc@role}\fpdoc@leaddisplay{\fpdoc@name}}%
  \iffpletter@window\ifx\fpletter@to\@empty\else
    %% DIN 5008, form B: the field starts 25 mm from the left and 45 mm from the top;
    %% the rule that opens the letter waits below it
    \AddToHookNext{shipout/foreground}{\put(25mm,-50mm){\parbox[t]{75mm}{\raggedright
      \ifx\fpletter@from\@empty\else{\fpdoc@meta{\fpsize{xxs}\fpletter@from}}\par\vspace{\fpspace{1}}\fi
      {\color{fpInk}\fpletter@to}}}}%
    \par\penalty\@M \vskip\dimexpr 96mm-\fpspace{7}-\pagetotal\relax
  \fi\fi
  \ifnum\fpintensity<4 \fpdoc@put{\fptickrule{\fpdoc@width}{\fpdoc@gutter}}\fi
  \nobreak\vskip\fpspace{3}\nobreak
  \iffpletter@window\else\ifx\fpletter@to\@empty\else
    \fpdoc@labelrow{\fplabel{To}}{{\color{fpInk}\fpletter@to}}\nobreak\vskip\fpspace{3}%
  \fi\fi
  \fpdoc@labelrow{\fplabel{Date}}{\@date}%
  \ifx\fpletter@re\@empty\else
    \nobreak\vskip\fpspace{1}%
    \fpdoc@labelrow{\fplabel{Re}}{{\color{fpInk}\bfseries\fpletter@re}}%
  \fi
  \par\vskip\fpspace{5}%
  \global\fpdoc@noroomfalse
  \setlength\parskip{\fpspace{3}}\setlength\leftskip{\fpdoc@indent}\prevdepth\z@}

%% \closing{<Kind regards,>} — room to sign by hand, then the name and the role.
\newcommand\closing[1]{%
  \par\addvspace{\fpspace{3}}#1\par
  \nobreak\vspace{\fpspace{7}}%
  {\color{fpInk}\bfseries\fpdoc@name}\par\nobreak
  \vspace{-\parskip}\fplabel{\fpdoc@role}\par}

%% \enclosures{<CV, references>} — a labelled row after the closing.
\newcommand\enclosures[1]{%
  \par\addvspace{\fpspace{5}}%
  \fpdoc@labelrow{\fplabel{Encl.}}{#1}}

\endinput
