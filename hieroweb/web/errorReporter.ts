export interface ErrorReporter {
    reportError(message: string) : void;
}

export class ConsoleErrorReporter implements ErrorReporter {
    public static instance: ConsoleErrorReporter = new ConsoleErrorReporter();

    private constructor() {}

    public reportError(message: string) : void {
        console.log(message);
    }
}