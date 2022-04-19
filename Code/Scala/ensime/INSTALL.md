
### ENSIME Installation and Configuration for Emacs

1. Copy

```
  addSbtPlugin("org.ensime" % "sbt-ensime" % "2.5.1")
```

into `~/.sbt/1.0/plugins/plugins.sbt`

2. Copy

```
  (use-package ensime
    :ensure t
    :pin melpa)
  (setq ensime-startup-notification nil)
  (defun mark-whole-buffer ()
    "Put point at beginning and mark at end of buffer.
  If narrowing is in effect, only uses the accessible part of the buffer.
  You probably should not use this function in Lisp programs;
  it is usually a mistake for a Lisp function to use any subroutine
  that uses or sets the mark."
    (declare (interactive-only t))
    (interactive)
    (push-mark)
    (push-mark (point-max) nil t)
    ;; This is really `point-min' in most cases, but if we're in the
    ;; minibuffer, this is at the end of the prompt.
    (goto-char (minibuffer-prompt-end)))
```

into your `~/.emacs file`

3. Unzip **ensime-20180615.1330.zip** into `~/.emacs.d/elpa/`

4. In the root folder of your project open a *sbt* console typing:

  `$ sbt`

inside the console type:

  `> ensimeConfig`

exit with `ctrl+d`.

5. Open a *scala* file with *emacs*...

6. Type `M-x ensime` to start the server...

7. Enjoy

