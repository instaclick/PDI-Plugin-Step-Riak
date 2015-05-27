package com.instaclick.pentaho.plugin.riak;

import com.basho.riak.client.core.query.Namespace;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;

import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import static com.instaclick.pentaho.plugin.riak.Messages.getString;

public class RiakPluginDialog extends BaseStepDialog implements StepDialogInterface
{
    private RiakPluginMeta input;

    private Label labelUri;
    private Text textUri;
    private FormData formUriLabel;
    private FormData formUriText;

    private Label    labelMode;
    private CCombo   comboMode;
    private FormData formModeLabel;
    private FormData formModeCombo;

    private Label labelBucket;
    private Text textBucket;
    private FormData formBucketLabel;
    private FormData formBucketText;
    
    private Label labelBucketType;
    private Text textBucketType;
    private FormData formBucketTypeLabel;
    private FormData formBucketTypeText;

    private Label labelValue;
    private Text textValue;
    private FormData formValueLabel;
    private FormData formValueText;

    private Label labelKey;
    private Text textKey;
    private FormData formKeyLabel;
    private FormData formKeyText;

    private Label labelResolver;
    private Text textResolver;
    private FormData formResolverLabel;
    private FormData formResolverText;

    private Label labelVClock;
    private Text textVClock;
    private FormData formVClockLabel;
    private FormData formVClockText;

    private Label labelContentType;
    private Text textContentType;
    private FormData formContentTypeLabel;
    private FormData formContentTypeText;

    private static final List<String> modes = new ArrayList<String>(Arrays.asList(new String[] {
        RiakPluginData.Mode.DELETE.toString(),
        RiakPluginData.Mode.GET.toString(),
        RiakPluginData.Mode.PUT.toString(),
    }));

    private final ModifyListener modifyListener = new ModifyListener() {
        @Override
        public void modifyText(ModifyEvent e) {
            input.setChanged();
        }
    };

    private final SelectionAdapter selectionModifyListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
            input.setChanged();
        }
    };

    private final SelectionAdapter comboModeListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
            textValue.setEnabled(false);
            textVClock.setEnabled(false);
            textResolver.setEnabled(false);

            if ( ! RiakPluginData.Mode.DELETE.toString().equals(comboMode.getText())) {
                textValue.setEnabled(true);
                textVClock.setEnabled(true);
            }

            if (RiakPluginData.Mode.GET.toString().equals(comboMode.getText())) {
                textResolver.setEnabled(true);
            }
        }
    };

    public RiakPluginDialog(Shell parent, Object in, TransMeta transMeta, String sname)
    {
        super(parent, (BaseStepMeta) in, transMeta, sname);

        input = (RiakPluginMeta) in;
    }

    @Override
    public String open()
    {
        Shell parent    = getParent();
        Display display = parent.getDisplay();
        shell           = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        props.setLook(shell);
        setShellImage(shell, input);

        changed = input.hasChanged();

        FormLayout formLayout   = new FormLayout();
        formLayout.marginWidth  = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(getString("RiakPlugin.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(getString("RiakPlugin.StepName.Label"));
        props.setLook(wlStepname);

        fdlStepname         = new FormData();
        fdlStepname.left    = new FormAttachment(0, 0);
        fdlStepname.right   = new FormAttachment(middle, -margin);
        fdlStepname.top     = new FormAttachment(0, margin);

        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(modifyListener);

        fdStepname       = new FormData();
        fdStepname.left  = new FormAttachment(middle, 0);
        fdStepname.top   = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);

        wStepname.setLayoutData(fdStepname);

        // Mode
        labelMode = new Label(shell, SWT.RIGHT);
        labelMode.setText(getString("RiakPlugin.Mode.Label"));
        props.setLook(labelMode);

        formModeLabel       = new FormData();
        formModeLabel.left  = new FormAttachment(0, 0);
        formModeLabel.top   = new FormAttachment(wStepname, margin);
        formModeLabel.right = new FormAttachment(middle, 0);

        labelMode.setLayoutData(formModeLabel);

        comboMode = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);

        comboMode.setToolTipText(getString("RiakPlugin.Mode.Label"));
        comboMode.addSelectionListener(comboModeListener);
        comboMode.addSelectionListener(selectionModifyListener);
        comboMode.setItems(modes.toArray(new String[modes.size()]));
        props.setLook(comboMode);

        formModeCombo      = new FormData();
        formModeCombo.left = new FormAttachment(middle, margin);
        formModeCombo.top  = new FormAttachment(wStepname, margin);
        formModeCombo.right= new FormAttachment(100, 0);

        comboMode.setLayoutData(formModeCombo);

        // Host line
        labelUri = new Label(shell, SWT.RIGHT);
        labelUri.setText(getString("RiakPlugin.Uri.Label"));
        props.setLook(labelUri);

        formUriLabel       = new FormData();
        formUriLabel.left  = new FormAttachment(0, 0);
        formUriLabel.right = new FormAttachment(middle, -margin);
        formUriLabel.top   = new FormAttachment(comboMode , margin);

        labelUri.setLayoutData(formUriLabel);

        textUri = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textUri);
        textUri.addModifyListener(modifyListener);

        formUriText        = new FormData();
        formUriText.left   = new FormAttachment(middle, 0);
        formUriText.right  = new FormAttachment(100, 0);
        formUriText.top    = new FormAttachment(comboMode, margin);

        textUri.setLayoutData(formUriText);

        // Bucket line
        labelBucket = new Label(shell, SWT.RIGHT);
        labelBucket.setText(getString("RiakPlugin.Bucket.Label"));
        props.setLook(labelBucket);

        formBucketLabel       = new FormData();
        formBucketLabel.left  = new FormAttachment(0, 0);
        formBucketLabel.right = new FormAttachment(middle, -margin);
        formBucketLabel.top   = new FormAttachment(textUri , margin);

        labelBucket.setLayoutData(formBucketLabel);

        textBucket = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textBucket);
        textBucket.addModifyListener(modifyListener);

        formBucketText        = new FormData();
        formBucketText.left   = new FormAttachment(middle, 0);
        formBucketText.right  = new FormAttachment(100, 0);
        formBucketText.top    = new FormAttachment(textUri, margin);

        textBucket.setLayoutData(formBucketText);
        
        // BucketType line
        labelBucketType = new Label(shell, SWT.RIGHT);
        labelBucketType.setText(getString("RiakPlugin.BucketType.Label"));
        props.setLook(labelBucketType);

        formBucketTypeLabel       = new FormData();
        formBucketTypeLabel.left  = new FormAttachment(0, 0);
        formBucketTypeLabel.right = new FormAttachment(middle, -margin);
        formBucketTypeLabel.top   = new FormAttachment(textBucket , margin);

        labelBucketType.setLayoutData(formBucketTypeLabel);

        textBucketType = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textBucketType);
        textBucketType.addModifyListener(modifyListener);

        formBucketTypeText        = new FormData();
        formBucketTypeText.left   = new FormAttachment(middle, 0);
        formBucketTypeText.right  = new FormAttachment(100, 0);
        formBucketTypeText.top    = new FormAttachment(textBucket, margin);

        textBucketType.setLayoutData(formBucketTypeText);

         // Body line
        labelValue = new Label(shell, SWT.RIGHT);
        labelValue.setText(getString("RiakPlugin.Value.Label"));
        props.setLook(labelValue);

        formValueLabel       = new FormData();
        formValueLabel.left  = new FormAttachment(0, 0);
        formValueLabel.right = new FormAttachment(middle, -margin);
        formValueLabel.top   = new FormAttachment(textBucketType , margin);

        labelValue.setLayoutData(formValueLabel);

        textValue = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textValue);
        textValue.addModifyListener(modifyListener);

        formValueText        = new FormData();
        formValueText.left   = new FormAttachment(middle, 0);
        formValueText.right  = new FormAttachment(100, 0);
        formValueText.top    = new FormAttachment(textBucketType, margin);

        textValue.setLayoutData(formValueText);

        // Key line
        labelKey = new Label(shell, SWT.RIGHT);
        labelKey.setText(getString("RiakPlugin.Key.Label"));
        props.setLook(labelKey);

        formKeyLabel       = new FormData();
        formKeyLabel.left  = new FormAttachment(0, 0);
        formKeyLabel.right = new FormAttachment(middle, -margin);
        formKeyLabel.top   = new FormAttachment(textValue , margin);

        labelKey.setLayoutData(formKeyLabel);

        textKey = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textKey);
        textKey.addModifyListener(modifyListener);

        formKeyText        = new FormData();
        formKeyText.left   = new FormAttachment(middle, 0);
        formKeyText.right  = new FormAttachment(100, 0);
        formKeyText.top    = new FormAttachment(textValue, margin);

        textKey.setLayoutData(formKeyText);

        // Resolver line
        labelResolver = new Label(shell, SWT.RIGHT);
        labelResolver.setText(getString("RiakPlugin.Resolver.Label"));
        props.setLook(labelResolver);

        formResolverLabel       = new FormData();
        formResolverLabel.left  = new FormAttachment(0, 0);
        formResolverLabel.right = new FormAttachment(middle, -margin);
        formResolverLabel.top   = new FormAttachment(textKey , margin);

        labelResolver.setLayoutData(formResolverLabel);

        textResolver = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textResolver);
        textResolver.addModifyListener(modifyListener);

        formResolverText        = new FormData();
        formResolverText.left   = new FormAttachment(middle, 0);
        formResolverText.right  = new FormAttachment(100, 0);
        formResolverText.top    = new FormAttachment(textKey, margin);

        textResolver.setLayoutData(formResolverText);

        // VClock line
        labelVClock = new Label(shell, SWT.RIGHT);
        labelVClock.setText(getString("RiakPlugin.VClock.Label"));
        props.setLook(labelVClock);

        formVClockLabel       = new FormData();
        formVClockLabel.left  = new FormAttachment(0, 0);
        formVClockLabel.right = new FormAttachment(middle, -margin);
        formVClockLabel.top   = new FormAttachment(textResolver , margin);

        labelVClock.setLayoutData(formVClockLabel);

        textVClock = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textVClock);
        textVClock.addModifyListener(modifyListener);

        formVClockText        = new FormData();
        formVClockText.left   = new FormAttachment(middle, 0);
        formVClockText.right  = new FormAttachment(100, 0);
        formVClockText.top    = new FormAttachment(textResolver, margin);

        textVClock.setLayoutData(formVClockText);

        // ContentType line
        labelContentType = new Label(shell, SWT.RIGHT);
        labelContentType.setText(getString("RiakPlugin.ContentType.Label"));
        props.setLook(labelContentType);

        formContentTypeLabel       = new FormData();
        formContentTypeLabel.left  = new FormAttachment(0, 0);
        formContentTypeLabel.right = new FormAttachment(middle, -margin);
        formContentTypeLabel.top   = new FormAttachment(textVClock , margin);

        labelContentType.setLayoutData(formContentTypeLabel);

        textContentType = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textContentType);
        textContentType.addModifyListener(modifyListener);

        formContentTypeText        = new FormData();
        formContentTypeText.left   = new FormAttachment(middle, 0);
        formContentTypeText.right  = new FormAttachment(100, 0);
        formContentTypeText.top    = new FormAttachment(textVClock, margin);

        textContentType.setLayoutData(formContentTypeText);

        // Some buttons
        wOK     = new Button(shell, SWT.PUSH);
        wCancel = new Button(shell, SWT.PUSH);

        wOK.setText(getString("System.Button.OK"));
        wCancel.setText(getString("System.Button.Cancel"));

        setButtonPositions(new Button[] { wOK, wCancel }, margin, null);

        // Add listeners
        lsCancel = new Listener() {
            @Override
            public void handleEvent(Event e) {
                cancel();
            }
        };

        lsOK = new Listener() {
            @Override
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        textValue.addSelectionListener(lsDef);
        wStepname.addSelectionListener(lsDef);
        textUri.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return stepname;
    }

    // Read data from input (TextFileInputInfo)
    public void getData()
    {
        wStepname.selectAll();

        int index = modes.indexOf(input.getMode().toString());

        if (index == -1) {
            index = 0;
        }

        comboMode.select(index);
        textValue.setEnabled(false);
        textVClock.setEnabled(false);
        textResolver.setEnabled(false);

        if (RiakPluginData.Mode.DELETE != input.getMode()) {
            textValue.setEnabled(true);
            textVClock.setEnabled(true);
        }

        if (RiakPluginData.Mode.GET == input.getMode()) {
            textResolver.setEnabled(true);
        }

        if (input.getResolver() != null) {
            textResolver.setText(input.getResolver());
        }

        if (input.getBucket() != null) {
            textBucket.setText(input.getBucket());
        }

        if (input.getBucketType() != null) {
            textBucketType.setText(input.getBucketType());
        }

        if (input.getContentType() != null) {
            textContentType.setText(input.getContentType());
        }

        if (input.getVClock() != null) {
            textVClock.setText(input.getVClock());
        }

        if (input.getUri() != null) {
            textUri.setText(input.getUri());
        }

        if (input.getValue() != null) {
            textValue.setText(input.getValue());
        }

        if (input.getKey() != null) {
            textKey.setText(input.getKey());
        }

        wStepname.selectAll();
    }

    private void cancel()
    {
        stepname = null;
        input.setChanged(changed);

        dispose();
    }

    private void ok()
    {
        stepname = wStepname.getText();

        if (Const.isEmpty(textUri.getText())) {
            textUri.setFocus();
            return;
        }

        if (Const.isEmpty(textBucket.getText())) {
            textBucket.setFocus();
            return;
        }
        
        if (Const.isEmpty(textBucketType.getText())) {
            textBucketType.setText(Namespace.DEFAULT_BUCKET_TYPE);
        }

        if (Const.isEmpty(textKey.getText())) {
            textKey.setFocus();
            return;
        }

        input.setResolver(textResolver.getText());
        input.setBucket(textBucket.getText());
        input.setVClock(textVClock.getText());
        input.setUri(textUri.getText());
        input.setMode(comboMode.getText());
        input.setValue(textValue.getText());
        input.setBucketType(textBucketType.getText());
        input.setContentType(textContentType.getText());
        input.setKey(textKey.getText());

        dispose();
    }
}
